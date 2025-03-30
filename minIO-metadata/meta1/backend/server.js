        const express = require('express');
        const cors = require('cors');
        const bodyParser = require('body-parser');
        require('dotenv').config();
        const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
        const fs = require('fs');
        const path = require('path');
        const parquet = require('parquets');
        const delta = require('delta');

        const app = express();

        // Middleware
        app.use(cors());
        app.use(bodyParser.json());

        // Initialize S3 Client
        const s3 = new S3Client({
            region: 'us-east-1', // Default region
            endpoint: process.env.MINIO_ENDPOINT || 'http://127.0.0.1:9000', // MinIO endpoint from .env
            forcePathStyle: true, // Required for MinIO
            credentials: {
                accessKeyId: process.env.MINIO_ACCESS_KEY, // MinIO access key from .env
                secretAccessKey: process.env.MINIO_SECRET_KEY, // MinIO secret key from .env
            },
        });

        // Test Route
        app.get('/', (req, res) => {
            res.send('Backend is working!');
        });

        // List Local Files Route
        app.get('/list-local-files', (req, res) => {
            const dirPath = path.resolve(process.env.LOCAL_DATA_PATH || './data'); // Path from .env or default
            fs.readdir(dirPath, (err, files) => {
                if (err) {
                    return res.status(500).send(err.message);
                }
                res.json(files);
            });
        });

        // List S3 Bucket Files Route
        app.get('/list-files', async (req, res) => {
            const { bucketName, prefix } = req.query; // e.g., ?bucketName=my-bucket&prefix=my-folder/

            if (!bucketName) {
                return res.status(400).send("Bucket name is required.");
            }

            try {
                const command = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: prefix || '',
                });
                const response = await s3.send(command);
                const files = response.Contents ? response.Contents.map((file) => file.Key) : [];
                res.json(files);
            } catch (error) {
                res.status(500).send(error.message);
            }
        });

        // Metadata Extraction for Text Files in S3 Bucket
        app.get('/metadata/txt/bucket', async (req, res) => {
            const { bucketName, fileKey } = req.query; // e.g., ?bucketName=my-bucket&fileKey=my-folder/my-file.txt

            if (!bucketName || !fileKey) {
                return res.status(400).send("Bucket name and file key are required.");
            }

            try {
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);

                let fileContent = '';
                await new Promise((resolve, reject) => {
                    response.Body.on('data', (chunk) => (fileContent += chunk.toString()));
                    response.Body.on('end', resolve);
                    response.Body.on('error', reject);
                });

                const lines = fileContent.split('\n');
                const words = fileContent.split(/\s+/).filter(Boolean);

                const metadata = {
                    bucketName,
                    fileKey,
                    size: response.ContentLength,
                    contentType: response.ContentType,
                    lineCount: lines.length,
                    wordCount: words.length,
                    characterCount: fileContent.length,
                };

                res.json(metadata);
            } catch (error) {
                res.status(500).send(error.message);
            }
        });

        // Metadata Extraction for Parquet Files in S3 Bucket
        app.get('/metadata/parquet/bucket', async (req, res) => {
            const { bucketName, fileKey } = req.query; // e.g., ?bucketName=my-bucket&fileKey=my-file.parquet

            if (!bucketName || !fileKey) {
                return res.status(400).send("Bucket name and file key are required.");
            }

            try {
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);

                const filePath = path.join(__dirname, 'temp.parquet');
                const writeStream = fs.createWriteStream(filePath);
                await new Promise((resolve, reject) => {
                    response.Body.pipe(writeStream);
                    response.Body.on('end', resolve);
                    response.Body.on('error', reject);
                });

                const reader = await parquet.ParquetReader.openFile(filePath);
                const schema = reader.getSchema();
                const rowCount = reader.getRowCount();
                await reader.close();

                fs.unlinkSync(filePath);

                res.json({
                    bucketName,
                    fileKey,
                    schema: schema.fields, // Column details
                    rowCount, // Total rows
                });
            } catch (error) {
                res.status(500).send(error.message);
            }
        });


        // List Parquet Files in a Bucket
        app.get('/list-parquet-files', async (req, res) => {
            const { bucketName, prefix } = req.query; // e.g., ?bucketName=data&prefix=folder/

            if (!bucketName) {
                return res.status(400).send("Bucket name is required.");
            }

            try {
                const command = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: prefix || '', // List files under the specified prefix
                });
                const response = await s3.send(command);

                // Filter only .parquet files
                const parquetFiles = response.Contents
                    ? response.Contents.filter((file) => file.Key.endsWith('.parquet'))
                    : [];

                res.json(parquetFiles.map((file) => ({ key: file.Key, size: file.Size })));
            } catch (error) {
                res.status(500).send(error.message);
            }
        });

        app.get('/list-delta-files', async (req, res) => {
            const { bucketName, prefix } = req.query; // e.g., ?bucketName=data&prefix=folder/

            if (!bucketName) {
                return res.status(400).send("Bucket name is required.");
            }

            try {
                const command = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: prefix || '', // List files under the specified prefix
                });
                const response = await s3.send(command);

                // Filter only .parquet files
                const parquetFiles = response.Contents
                    ? response.Contents.filter((file) => file.Key.endsWith('.dlt'))
                    : [];

                res.json(parquetFiles.map((file) => ({ key: file.Key, size: file.Size })));
            } catch (error) {
                res.status(500).send(error.message);
            }
        });

        // Metadata Extraction for Delta Tables
        app.get('/metadata/delta/bucket', async (req, res) => {
            const { bucketName, fileKey } = req.query;
            if (!bucketName || !fileKey) {
                return res.status(400).send('Bucket name and file key are required.');
            }

            try {
                const filePath = await fetchS3File(bucketName, fileKey);

                // Replace with actual Delta metadata extraction logic
                const deltaMetadata = { message: 'Delta metadata extraction not implemented yet.' };

                fs.unlinkSync(filePath);

                res.json({ bucketName, fileKey, deltaMetadata });
            } catch (error) {
                res.status(500).send(error.message);
            }
        });


        // View Metadata for a Specific Parquet File
        app.get('/view-parquet-metadata', async (req, res) => {
            const { bucketName, fileKey } = req.query; // e.g., ?bucketName=data&fileKey=file.parquet
            let tempFilePath = null;

            if (!bucketName || !fileKey) {
                console.error('Missing parameters:', { bucketName, fileKey });
                return res.status(400).send("Bucket name and file key are required.");
            }

            try {
                console.log('Fetching parquet file:', { bucketName, fileKey });
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);
                console.log('S3 response received');

                tempFilePath = path.join(__dirname, `temp_${Date.now()}.parquet`);
                console.log('Saving to temporary file:', tempFilePath);
                
                const writeStream = fs.createWriteStream(tempFilePath);
                await new Promise((resolve, reject) => {
                    response.Body.pipe(writeStream);
                    response.Body.on('end', () => {
                        console.log('File download completed');
                        resolve();
                    });
                    response.Body.on('error', (err) => {
                        console.error('Error downloading file:', err);
                        reject(err);
                    });
                });

                console.log('Opening parquet file');
                const reader = await parquet.ParquetReader.openFile(tempFilePath);
                console.log('Getting schema');
                const schema = reader.getSchema();
                console.log('Schema:', schema);
                console.log('Getting row count');
                const rowCount = reader.getRowCount();
                console.log('Row count:', rowCount);
                await reader.close();

                console.log('Cleaning up temporary file');
                fs.unlinkSync(tempFilePath);
                tempFilePath = null;

                const result = {
                    bucketName,
                    fileKey,
                    schema: schema.fields, // Column details
                    rowCount, // Total rows
                };
                console.log('Sending response:', result);
                res.json(result);
            } catch (error) {
                console.error('Error processing parquet file:', error);
                if (error.name === 'NoSuchKey') {
                    res.status(404).send('Parquet file not found in the specified bucket');
                } else if (error.name === 'NoSuchBucket') {
                    res.status(404).send('Specified bucket not found');
                } else {
                    res.status(500).send(`Error processing parquet file: ${error.message}`);
                }
            } finally {
                // Clean up temporary file if it exists
                if (tempFilePath && fs.existsSync(tempFilePath)) {
                    try {
                        fs.unlinkSync(tempFilePath);
                        console.log('Cleaned up temporary file in finally block');
                    } catch (error) {
                        console.error('Error cleaning up temporary file:', error);
                    }
                }
            }
        });
        // View Metadata for a Specific Parquet File
        app.get('/view-delta-metadata', async (req, res) => {
            const { bucketName, fileKey } = req.query; // e.g., ?bucketName=data&fileKey=file.delta
        
            if (!bucketName || !fileKey) {
                return res.status(400).send("Bucket name and file key are required.");
            }
        
            try {
                // Get the file from S3
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);
        
                // Save the file locally as a temporary Delta file
                const filePath = path.join(__dirname, 'temp.dlt');
                const writeStream = fs.createWriteStream(filePath);
                await new Promise((resolve, reject) => {
                    response.Body.pipe(writeStream);
                    response.Body.on('end', resolve);
                    response.Body.on('error', reject);
                });
        
                // Read Delta metadata (you'll need to install a Delta Lake reader library)
                // Example with delta-rs:
                const delta = require('delta');
                const deltaTable = await delta.DeltaTable.open(filePath);
        
                // Extract metadata
                const schema = deltaTable.schema();
                const version = deltaTable.version();
                const files = deltaTable.files();
        
                // Clean up the temporary file
                fs.unlinkSync(filePath);
        
                // Send metadata as the response
                res.json({
                    bucketName,
                    fileKey,
                    schema,
                    version,
                    files,
                });
            } catch (error) {
                console.error("Error reading Delta metadata:", error);
                res.status(500).send(error.message);
            }
        });
        

        // Convert Parquet to Delta and Get Metadata
        app.get('/convert-to-delta', async (req, res) => {
            const { bucketName, fileKey } = req.query;
            let tempParquetPath = null;

            if (!bucketName || !fileKey) {
                return res.status(400).send("Bucket name and file key are required.");
            }

            try {
                // Download Parquet file
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);

                // Create a unique temporary file path
                tempParquetPath = path.join(__dirname, `temp_${Date.now()}.parquet`);
                
                // Save the file using a buffer to ensure complete download
                const chunks = [];
                for await (const chunk of response.Body) {
                    chunks.push(chunk);
                }
                const buffer = Buffer.concat(chunks);
                fs.writeFileSync(tempParquetPath, buffer);

                // Read Parquet schema and data
                const reader = await parquet.ParquetReader.openFile(tempParquetPath);
                const parquetSchema = reader.getSchema();
                const rowCount = reader.getRowCount();
                await reader.close();

                // Create Delta table directory structure
                const tableName = path.basename(fileKey, '.parquet');
                const deltaTablePath = `${fileKey.replace('.parquet', '_delta')}`;
                const deltaLogPath = `${deltaTablePath}/_delta_log`;

                // Convert Parquet schema to Delta schema format
                const deltaSchema = {
                    type: 'struct',
                    fields: Object.entries(parquetSchema).map(([name, type]) => ({
                        name,
                        type: typeof type === 'string' ? type.toLowerCase() : 'string',
                        nullable: true,
                        metadata: {}
                    }))
                };

                // Create Delta log files
                const deltaLogFiles = [
                    {
                        key: `${deltaLogPath}/00000000000000000000.json`,
                        content: JSON.stringify({
                            commitInfo: {
                                timestamp: Date.now(),
                                operation: 'CREATE TABLE',
                                operationParameters: {
                                    mode: 'create',
                                    partitionBy: '[]',
                                },
                                readVersion: -1,
                                isolationLevel: 'Serializable',
                                isBlindAppend: true,
                            },
                            protocol: {
                                minReaderVersion: 1,
                                minWriterVersion: 2,
                            },
                            metaData: {
                                id: Date.now().toString(),
                                name: tableName,
                                description: `Delta table converted from ${fileKey}`,
                                format: {
                                    provider: 'parquet',
                                    options: {}
                                },
                                schemaString: JSON.stringify(deltaSchema),
                                partitionColumns: [],
                                configuration: {
                                    'delta.enableChangeDataFeed': 'true',
                                    'delta.minReaderVersion': '1',
                                    'delta.minWriterVersion': '2',
                                    'delta.columnMapping.mode': 'name',
                                },
                                createdTime: Date.now(),
                            },
                        }),
                    },
                    {
                        key: `${deltaLogPath}/00000000000000000001.json`,
                        content: JSON.stringify({
                            commitInfo: {
                                timestamp: Date.now(),
                                operation: 'WRITE',
                                operationParameters: {
                                    mode: 'append',
                                    partitionBy: '[]',
                                },
                                readVersion: 0,
                                isolationLevel: 'Serializable',
                                isBlindAppend: true,
                            },
                            protocol: {
                                minReaderVersion: 1,
                                minWriterVersion: 2,
                            },
                            add: [{
                                path: fileKey,
                                size: buffer.length,
                                modificationTime: Date.now(),
                                dataChange: true,
                                stats: {
                                    numRecords: rowCount,
                                    minValues: {},
                                    maxValues: {},
                                    nullCount: {}
                                }
                            }],
                        }),
                    },
                ];

                // Upload Delta log files
                for (const file of deltaLogFiles) {
                    await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: file.key,
                        Body: file.content,
                        ContentType: 'application/json',
                    }));
                }

                // Create Delta table metadata
                const deltaMetadata = {
                    tableName,
                    location: `s3://${bucketName}/${deltaTablePath}`,
                    schema: deltaSchema,
                    configuration: {
                        'delta.enableChangeDataFeed': 'true',
                        'delta.minReaderVersion': '1',
                        'delta.minWriterVersion': '2',
                        'delta.columnMapping.mode': 'name',
                    },
                    version: 1,
                    timestamp: Date.now(),
                    commitInfo: {
                        timestamp: Date.now(),
                        operation: 'CREATE TABLE',
                        operationParameters: {
                            mode: 'create',
                            partitionBy: '[]',
                        },
                        readVersion: -1,
                        isolationLevel: 'Serializable',
                        isBlindAppend: true,
                    },
                    protocol: {
                        minReaderVersion: 1,
                        minWriterVersion: 2,
                    },
                    metadataLog: {
                        path: `s3://${bucketName}/${deltaLogPath}`,
                        version: 1,
                        size: deltaLogFiles.reduce((acc, file) => acc + file.content.length, 0),
                    },
                    files: [{
                        path: fileKey,
                        size: buffer.length,
                        modificationTime: Date.now(),
                        dataChange: true,
                        stats: {
                            numRecords: rowCount,
                            minValues: {},
                            maxValues: {},
                            nullCount: {}
                        }
                    }],
                    statistics: {
                        numFiles: 1,
                        numRecords: rowCount,
                        totalSize: buffer.length,
                        averageRecordSize: buffer.length / rowCount
                    }
                };

                res.json(deltaMetadata);
            } catch (error) {
                console.error('Error converting to Delta:', error);
                res.status(500).send(`Error converting to Delta: ${error.message}`);
            } finally {
                // Clean up temporary file
                if (tempParquetPath && fs.existsSync(tempParquetPath)) {
                    try {
                        fs.unlinkSync(tempParquetPath);
                    } catch (error) {
                        console.error('Error cleaning up temporary file:', error);
                    }
                }
            }
        });

        // Convert Parquet to Iceberg and Get Metadata
        app.get('/convert-to-iceberg', async (req, res) => {
            const { bucketName, fileKey } = req.query;
            let tempParquetPath = null;

            if (!bucketName || !fileKey) {
                return res.status(400).send("Bucket name and file key are required.");
            }

            try {
                // Download Parquet file
                const command = new GetObjectCommand({
                    Bucket: bucketName,
                    Key: fileKey,
                });
                const response = await s3.send(command);

                // Create a unique temporary file path
                tempParquetPath = path.join(__dirname, `temp_${Date.now()}.parquet`);
                
                // Save the file using a buffer to ensure complete download
                const chunks = [];
                for await (const chunk of response.Body) {
                    chunks.push(chunk);
                }
                const buffer = Buffer.concat(chunks);
                fs.writeFileSync(tempParquetPath, buffer);

                // Read Parquet schema and data
                const reader = await parquet.ParquetReader.openFile(tempParquetPath);
                const parquetSchema = reader.getSchema();
                const rowCount = reader.getRowCount();
                await reader.close();

                // Create Iceberg table directory structure
                const tableName = path.basename(fileKey, '.parquet');
                const icebergTablePath = fileKey.replace('.parquet', '_iceberg');
                const metadataPath = `${icebergTablePath}/metadata`;

                // Create Iceberg metadata files
                const metadataFiles = [
                    {
                        key: `${metadataPath}/00000-${Date.now()}-metadata.json`,
                        content: JSON.stringify({
                            formatVersion: 1,
                            tableUuid: Date.now().toString(),
                            location: `s3://${bucketName}/${icebergTablePath}`,
                            lastUpdatedMs: Date.now(),
                            lastColumnId: Object.keys(parquetSchema).length,
                            schema: parquetSchema,
                            partitionSpec: [],
                            defaultSpecId: 0,
                            lastPartitionId: 0,
                            properties: {
                                'write.format.default': 'PARQUET',
                                'commit.retry.num-retries': '5',
                                'commit.retry.min-wait-ms': '100',
                                'commit.retry.max-wait-ms': '2000',
                            },
                            currentSchemaId: 0,
                            schemas: [{
                                schemaId: 0,
                                type: 'struct',
                                fields: Object.entries(parquetSchema).map(([name, type]) => ({
                                    id: Date.now() + Math.random(),
                                    name,
                                    required: true,
                                    type: typeof type === 'string' ? type.toLowerCase() : 'string',
                                })),
                            }],
                            defaultSpecId: 0,
                            partitionSpecs: [{
                                specId: 0,
                                fields: [],
                            }],
                            lastPartitionId: 0,
                            defaultSortOrderId: 0,
                            sortOrders: [{
                                orderId: 0,
                                fields: [],
                            }],
                        }),
                    },
                    {
                        key: `${metadataPath}/00001-${Date.now()}-snap-${Date.now()}.avro`,
                        content: JSON.stringify({
                            manifestList: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-manifest-list.avro`,
                            snapshotId: Date.now(),
                            timestamp: Date.now(),
                            summary: {
                                operation: 'append',
                                'spark.app.id': Date.now().toString(),
                                'added-data-files': '1',
                                'added-records': rowCount.toString(),
                                'added-files-size': buffer.length.toString(),
                                'changed-partition-count': '1',
                                'total-records': rowCount.toString(),
                                'total-files-size': buffer.length.toString(),
                                'total-data-files': '1',
                                'total-delete-files': '0',
                                'total-position-deletes': '0',
                                'total-equality-deletes': '0',
                            },
                            schemaId: 0,
                            manifestListLocation: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-manifest-list.avro`,
                            summaryLocation: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-summary.avro`,
                        }),
                    },
                ];

                // Upload Iceberg metadata files
                for (const file of metadataFiles) {
                    await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: file.key,
                        Body: file.content,
                        ContentType: 'application/json',
                    }));
                }

                // Create Iceberg table metadata
                const icebergMetadata = {
                    tableName,
                    location: `s3://${bucketName}/${icebergTablePath}`,
                    schema: parquetSchema,
                    partitionSpec: [],
                    properties: {
                        'write.format.default': 'PARQUET',
                        'commit.retry.num-retries': '5',
                        'commit.retry.min-wait-ms': '100',
                        'commit.retry.max-wait-ms': '2000',
                    },
                    currentSnapshot: {
                        snapshotId: Date.now(),
                        timestamp: Date.now(),
                        manifestList: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-manifest-list.avro`,
                    },
                    snapshots: [{
                        snapshotId: Date.now(),
                        timestamp: Date.now(),
                        manifestList: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-manifest-list.avro`,
                    }],
                    manifestFiles: [{
                        path: `s3://${bucketName}/${metadataPath}/00000-${Date.now()}-manifest.avro`,
                        length: buffer.length,
                        partitionSpecId: 0,
                        addedFiles: 1,
                        existingFiles: 0,
                        deletedFiles: 0,
                    }],
                    statistics: {
                        rowCount,
                        fileCount: 1,
                        totalSize: buffer.length,
                        averageRecordSize: buffer.length / rowCount
                    }
                };

                res.json(icebergMetadata);
            } catch (error) {
                console.error('Error converting to Iceberg:', error);
                res.status(500).send(`Error converting to Iceberg: ${error.message}`);
            } finally {
                // Clean up temporary file
                if (tempParquetPath && fs.existsSync(tempParquetPath)) {
                    try {
                        fs.unlinkSync(tempParquetPath);
                    } catch (error) {
                        console.error('Error cleaning up temporary file:', error);
                    }
                }
            }
        });

        // Get Iceberg Table Metadata
        app.get('/iceberg-metadata', async (req, res) => {
            const { bucketName, tableName } = req.query;

            if (!bucketName || !tableName) {
                return res.status(400).send("Bucket name and table name are required.");
            }

            try {
                // List files in the table directory
                const listCommand = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: `${tableName}/`,
                });
                const listResponse = await s3.send(listCommand);

                // Calculate statistics
                const files = listResponse.Contents || [];
                const totalSize = files.reduce((acc, file) => acc + (file.Size || 0), 0);
                const fileCount = files.length;

                // Create metadata response
                const metadata = {
                    tableName,
                    location: `s3://${bucketName}/${tableName}`,
                    schema: {}, // You would need to read this from the metadata files
                    partitionSpec: [],
                    properties: {
                        'write.format.default': 'PARQUET',
                    },
                    currentSnapshot: {
                        snapshotId: Date.now(),
                        timestamp: Date.now(),
                        manifestList: `s3://${bucketName}/${tableName}/metadata/${Date.now()}-manifest-list.avro`,
                    },
                    snapshots: [{
                        snapshotId: Date.now(),
                        timestamp: Date.now(),
                        manifestList: `s3://${bucketName}/${tableName}/metadata/${Date.now()}-manifest-list.avro`,
                    }],
                    manifestFiles: files.map(file => ({
                        path: `s3://${bucketName}/${file.Key}`,
                        length: file.Size || 0,
                        partitionSpecId: 0,
                        addedFiles: 1,
                        existingFiles: 0,
                        deletedFiles: 0,
                    })),
                    statistics: {
                        fileCount,
                        totalSize,
                        averageRecordSize: fileCount > 0 ? totalSize / fileCount : 0
                    }
                };

                res.json(metadata);
            } catch (error) {
                console.error('Error getting Iceberg metadata:', error);
                res.status(500).send(`Error getting Iceberg metadata: ${error.message}`);
            }
        });

        // Get Delta Table Metadata
        app.get('/delta-metadata', async (req, res) => {
            const { bucketName, tableName } = req.query;

            if (!bucketName || !tableName) {
                return res.status(400).send("Bucket name and table name are required.");
            }

            try {
                // List files in the table directory
                const listCommand = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: `${tableName}/`,
                });
                const listResponse = await s3.send(listCommand);

                // Calculate statistics
                const files = listResponse.Contents || [];
                const totalSize = files.reduce((acc, file) => acc + (file.Size || 0), 0);
                const fileCount = files.length;

                // Get the latest version from _delta_log
                const deltaLogPrefix = `${tableName}/_delta_log/`;
                const deltaLogCommand = new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: deltaLogPrefix,
                });
                const deltaLogResponse = await s3.send(deltaLogCommand);
                const deltaLogFiles = deltaLogResponse.Contents || [];
                const latestVersion = deltaLogFiles.length > 0 ? deltaLogFiles.length - 1 : 0;

                // Create metadata response
                const metadata = {
                    tableName,
                    location: `s3://${bucketName}/${tableName}`,
                    configuration: {
                        'delta.enableChangeDataFeed': 'true',
                        'delta.minReaderVersion': '1',
                        'delta.minWriterVersion': '2',
                        'delta.columnMapping.mode': 'name',
                    },
                    version: latestVersion,
                    timestamp: Date.now(),
                    protocol: {
                        minReaderVersion: 1,
                        minWriterVersion: 2,
                    },
                    metadataLog: {
                        path: `s3://${bucketName}/${tableName}/_delta_log`,
                        version: latestVersion,
                        size: deltaLogFiles.reduce((acc, file) => acc + (file.Size || 0), 0),
                    },
                    files: files.filter(file => !file.Key.startsWith(deltaLogPrefix)).map(file => ({
                        path: file.Key,
                        size: file.Size || 0,
                        modificationTime: file.LastModified?.getTime() || Date.now(),
                        dataChange: true,
                    })),
                    statistics: {
                        numFiles: fileCount,
                        totalSize,
                        averageFileSize: fileCount > 0 ? totalSize / fileCount : 0
                    }
                };

                res.json(metadata);
            } catch (error) {
                console.error('Error getting Delta metadata:', error);
                res.status(500).send(`Error getting Delta metadata: ${error.message}`);
            }
        });

        // Start the server
        const PORT = process.env.PORT || 5000; // Port from .env or default to 5000
        app.listen(PORT, () => {
            console.log(`Server is running on http://localhost:${PORT}`);
        });