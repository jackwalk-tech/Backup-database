const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
const archiver = require('archiver');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const AdmZip = require('adm-zip');
const fs = require('fs');
const { EJSON } = require('bson');

const app = express();
const PORT = 3000;

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({ 
  storage: storage,
  limits: { fileSize: 500 * 1024 * 1024 }, // 500MB limit
  fileFilter: (req, file, cb) => {
    if (path.extname(file.originalname).toLowerCase() === '.zip') {
      cb(null, true);
    } else {
      cb(new Error('Only .zip files are allowed'));
    }
  }
});

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Get collections list with document counts
app.post('/api/get-collections', async (req, res) => {
  const { mongoUrl, dbName } = req.body;
  let client;
  
  try {
    if (!mongoUrl || !dbName) {
      return res.status(400).json({ 
        error: 'MongoDB URL and Database Name are required' 
      });
    }

    client = new MongoClient(mongoUrl, {
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
    
    await client.connect();
    console.log('Connected to MongoDB');
    
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    
    if (collections.length === 0) {
      return res.json({ 
        success: true,
        collections: [],
        message: 'No collections found in database'
      });
    }

    // Get document count for each collection
    const collectionDetails = [];
    
    for (const col of collections) {
      try {
        const collection = db.collection(col.name);
        const count = await collection.countDocuments();
        
        collectionDetails.push({
          name: col.name,
          count: count,
          selected: true // Default all selected
        });
        
        console.log(`Collection "${col.name}": ${count} documents`);
      } catch (error) {
        console.error(`Error counting documents in ${col.name}:`, error);
        collectionDetails.push({
          name: col.name,
          count: 0,
          selected: true,
          error: 'Could not count documents'
        });
      }
    }
    
    res.json({ 
      success: true,
      collections: collectionDetails,
      totalCollections: collectionDetails.length,
      totalDocuments: collectionDetails.reduce((sum, col) => sum + col.count, 0)
    });
    
  } catch (error) {
    console.error('Error fetching collections:', error);
    res.status(500).json({ 
      error: 'Failed to fetch collections',
      details: error.message 
    });
  } finally {
    if (client) {
      await client.close();
      console.log('MongoDB connection closed');
    }
  }
});

// Backup selected collections
app.post('/api/backup', async (req, res) => {
  const { mongoUrl, dbName, selectedCollections } = req.body;
  const startTime = Date.now();
  
  let client;
  
  try {
    // Validate inputs
    if (!mongoUrl || !dbName) {
      return res.status(400).json({ 
        error: 'MongoDB URL and Database Name are required' 
      });
    }

    if (!selectedCollections || selectedCollections.length === 0) {
      return res.status(400).json({ 
        error: 'Please select at least one collection to backup' 
      });
    }

    // Connect to MongoDB
    client = new MongoClient(mongoUrl, {
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
    
    await client.connect();
    console.log('Connected to MongoDB for backup');
    
    const db = client.db(dbName);

    // Set response headers for zip download
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${dbName}_backup_${Date.now()}.zip"`);

    // Create archiver instance
    const archive = archiver('zip', {
      zlib: { level: 9 } // Maximum compression
    });

    // Pipe archive to response
    archive.pipe(res);

    // Process each selected collection
    let totalDocuments = 0;
    const collectionStats = [];

    for (const collectionName of selectedCollections) {
      try {
        const collection = db.collection(collectionName);
        
        // Get all documents from collection
        const documents = await collection.find({}).toArray();
        
        console.log(`Collection "${collectionName}": ${documents.length} documents`);
        
        totalDocuments += documents.length;
        collectionStats.push({
          name: collectionName,
          count: documents.length
        });

        // Use EJSON to serialize MongoDB types properly (ObjectId, Date, etc.)
        const jsonData = EJSON.stringify(documents, null, 2);
        archive.append(jsonData, { name: `${collectionName}.json` });
        
      } catch (collErr) {
        console.error(`Error processing collection ${collectionName}:`, collErr);
        // Add error log to zip
        archive.append(
          JSON.stringify({ error: collErr.message }, null, 2),
          { name: `${collectionName}_ERROR.json` }
        );
      }
    }

    // Create metadata file
    const metadata = {
      databaseName: dbName,
      backupDate: new Date().toISOString(),
      totalCollections: selectedCollections.length,
      totalDocuments: totalDocuments,
      collections: collectionStats,
      processingTimeMs: Date.now() - startTime,
      format: 'EJSON' // Indicate the format used
    };

    archive.append(JSON.stringify(metadata, null, 2), { name: '_backup_metadata.json' });

    // Finalize the archive
    await archive.finalize();
    
    console.log(`Backup completed in ${Date.now() - startTime}ms`);
    console.log(`Total documents backed up: ${totalDocuments}`);

  } catch (error) {
    console.error('Backup error:', error);
    
    // If headers not sent, send error response
    if (!res.headersSent) {
      return res.status(500).json({ 
        error: error.message || 'Failed to backup database',
        details: error.toString()
      });
    }
  } finally {
    // Close MongoDB connection
    if (client) {
      await client.close();
      console.log('MongoDB connection closed');
    }
  }
});

// Import database from ZIP file
app.post('/api/import', upload.single('zipFile'), async (req, res) => {
  const { mongoUrl, dbName } = req.body;
  const startTime = Date.now();
  
  let client;
  
  try {
    // Validate inputs
    if (!mongoUrl || !dbName) {
      return res.status(400).json({ 
        error: 'MongoDB URL and Database Name are required' 
      });
    }

    if (!req.file) {
      return res.status(400).json({ 
        error: 'No ZIP file uploaded' 
      });
    }

    console.log(`Starting import to database: ${dbName}`);
    console.log(`ZIP file size: ${(req.file.size / 1024 / 1024).toFixed(2)} MB`);

    // Extract ZIP file from buffer
    const zip = new AdmZip(req.file.buffer);
    const zipEntries = zip.getEntries();

    // Filter only JSON files (exclude metadata and error files)
    const jsonFiles = zipEntries.filter(entry => 
      !entry.isDirectory && 
      entry.entryName.endsWith('.json') &&
      !entry.entryName.startsWith('_backup_metadata') &&
      !entry.entryName.includes('_ERROR')
    );

    if (jsonFiles.length === 0) {
      return res.status(400).json({ 
        error: 'No valid JSON collection files found in ZIP' 
      });
    }

    console.log(`Found ${jsonFiles.length} collections to import`);

    // Connect to MongoDB
    client = new MongoClient(mongoUrl, {
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
    
    await client.connect();
    console.log('Connected to MongoDB for import');
    
    const db = client.db(dbName);

    // Import each collection
    const importResults = [];
    let totalDocumentsImported = 0;

    for (const entry of jsonFiles) {
      const collectionName = path.basename(entry.entryName, '.json');
      
      try {
        // Extract and parse JSON data
        const jsonData = entry.getData().toString('utf8');
        
        // Try to parse as EJSON first (to restore ObjectId and other BSON types)
        let documents;
        try {
          documents = EJSON.parse(jsonData);
        } catch (ejsonError) {
          // Fallback to regular JSON if EJSON parse fails
          console.log(`EJSON parse failed for ${collectionName}, trying regular JSON`);
          documents = JSON.parse(jsonData);
        }

        if (!Array.isArray(documents)) {
          throw new Error('JSON file must contain an array of documents');
        }

        if (documents.length === 0) {
          console.log(`Collection "${collectionName}": Skipping (empty)`);
          importResults.push({
            collection: collectionName,
            status: 'skipped',
            documentsImported: 0,
            message: 'Empty collection'
          });
          continue;
        }

        const collection = db.collection(collectionName);
        
        // Insert documents in batches for better performance
        const batchSize = 1000;
        let imported = 0;

        for (let i = 0; i < documents.length; i += batchSize) {
          const batch = documents.slice(i, i + batchSize);
          await collection.insertMany(batch, { ordered: false });
          imported += batch.length;
        }

        console.log(`Collection "${collectionName}": Imported ${imported} documents`);
        
        totalDocumentsImported += imported;
        importResults.push({
          collection: collectionName,
          status: 'success',
          documentsImported: imported
        });

      } catch (collErr) {
        console.error(`Error importing collection ${collectionName}:`, collErr);
        importResults.push({
          collection: collectionName,
          status: 'error',
          documentsImported: 0,
          error: collErr.message
        });
      }
    }

    const processingTime = Date.now() - startTime;

    console.log(`Import completed in ${processingTime}ms`);
    console.log(`Total documents imported: ${totalDocumentsImported}`);

    // Return success response
    res.json({
      success: true,
      databaseName: dbName,
      totalCollections: jsonFiles.length,
      successfulImports: importResults.filter(r => r.status === 'success').length,
      totalDocumentsImported: totalDocumentsImported,
      processingTimeMs: processingTime,
      results: importResults
    });

  } catch (error) {
    console.error('Import error:', error);
    res.status(500).json({ 
      error: error.message || 'Failed to import database',
      details: error.toString()
    });
  } finally {
    // Close MongoDB connection
    if (client) {
      await client.close();
      console.log('MongoDB connection closed');
    }
  }
});

app.listen(PORT, () => {
  console.log(`MongoDB Backup Server running on http://localhost:${PORT}`);
  console.log(`Ready to accept backup requests...`);
});