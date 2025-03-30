import React, { useState } from 'react';
import axios from 'axios';

const App = () => {
    const [bucketName, setBucketName] = useState('');
    const [fileKey, setFileKey] = useState('');
    const [metadata, setMetadata] = useState(null);
    const [error, setError] = useState(null);
    const [parquetData, setParquetData] = useState(null);
    const [icebergData, setIcebergData] = useState(null);
    const [deltaData, setDeltaData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [activeTab, setActiveTab] = useState('metadata');
    const [currentPage, setCurrentPage] = useState('home');
    const [viewerType, setViewerType] = useState('minio');

    const handleTabChange = (tab) => {
        setActiveTab(tab);
        setError(null); // Clear any previous errors when switching tabs
        setLoading(false); // Reset loading state when switching tabs
        
        // Reset data states for other tabs to ensure clean state
        if (tab !== 'metadata') setMetadata(null);
        if (tab !== 'parquet') setParquetData(null);
        if (tab !== 'iceberg') setIcebergData(null);
        if (tab !== 'delta') setDeltaData(null);
    };

    const handleFetchMetadata = async () => {
        if (!bucketName || !fileKey) {
            setError('Bucket name and file key are required.');
            return;
        }
        try {
            setLoading(true);
            setError(null);
            const response = await axios.get('http://localhost:5000/metadata/txt/bucket', {
                params: { bucketName, fileKey },
            });
            setMetadata(response.data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleViewParquet = async () => {
        if (!bucketName || !fileKey) {
            setError('Bucket name and file key are required.');
            return;
        }
        try {
            setLoading(true);
            setError(null);
            setParquetData(null); // Reset parquet data before fetching
            setActiveTab('parquet'); // Automatically switch to parquet tab
            console.log('Fetching parquet data for:', { bucketName, fileKey });
            const response = await axios.get('http://localhost:5000/view-parquet-metadata', {
                params: { bucketName, fileKey },
            });
            console.log('Received parquet response:', response.data);
            if (response.data) {
                setParquetData(response.data);
                setError(null);
            } else {
                setError('No data received from server');
                console.error('Empty response from server');
            }
        } catch (err) {
            console.error('Parquet view error:', err);
            setError(err.response?.data || err.message || 'Error fetching parquet data');
            setParquetData(null);
        } finally {
            setLoading(false);
        }
    };

    const handleConvertToIceberg = async () => {
        if (!bucketName || !fileKey) {
            setError('Bucket name and file key are required.');
            return;
        }
        try {
            setLoading(true);
            setError(null);
            setIcebergData(null); // Reset iceberg data before fetching
            setActiveTab('iceberg'); // Automatically switch to iceberg tab
            const response = await axios.get('http://localhost:5000/convert-to-iceberg', {
                params: { bucketName, fileKey },
            });
            if (response.data) {
            setIcebergData(response.data);
                setError(null);
            } else {
                setError('No data received from server');
            }
        } catch (err) {
            console.error('Iceberg conversion error:', err);
            setError(err.response?.data || err.message || 'Error converting to Iceberg');
            setIcebergData(null);
        } finally {
            setLoading(false);
        }
    };

    const handleConvertToDelta = async () => {
        if (!bucketName || !fileKey) {
            setError('Bucket name and file key are required.');
            return;
        }
        try {
            setLoading(true);
            setError(null);
            setDeltaData(null); // Reset delta data before fetching
            setActiveTab('delta'); // Automatically switch to delta tab
            const response = await axios.get('http://localhost:5000/convert-to-delta', {
                params: { bucketName, fileKey },
            });
            if (response.data) {
            setDeltaData(response.data);
                setError(null);
            } else {
                setError('No data received from server');
            }
        } catch (err) {
            console.error('Delta conversion error:', err);
            setError(err.response?.data || err.message || 'Error converting to Delta');
            setDeltaData(null);
        } finally {
            setLoading(false);
        }
    };

    const renderMetadataContent = () => {
        if (loading) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>Loading metadata...</p>
                </div>
            );
        }

        if (error) {
            return (
                <div className="text-center py-8">
                    <div className="p-4 bg-red-900/50 text-red-200 rounded-lg">
                        {error}
                    </div>
                </div>
            );
        }

        if (!metadata) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>No metadata available. Click "Fetch Metadata" to view the data.</p>
                </div>
            );
        }

        return (
            <div className="space-y-6">
                <div className="grid grid-cols-3 gap-4">
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">File Size</h3>
                        <p className="text-lg">{(metadata.size / 1024).toFixed(2)} KB</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Line Count</h3>
                        <p className="text-lg">{metadata.lineCount?.toLocaleString()}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Word Count</h3>
                        <p className="text-lg">{metadata.wordCount?.toLocaleString()}</p>
                    </div>
                </div>
                <div className="bg-[#242424] rounded overflow-hidden">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-left text-gray-400 border-b border-gray-800">
                                <th className="py-3 px-4 font-medium">Property</th>
                                <th className="py-3 px-4 font-medium">Value</th>
                            </tr>
                        </thead>
                        <tbody>
                            {Object.entries(metadata).map(([key, value]) => (
                                <tr key={key} className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">{key}</td>
                                    <td className="py-2 px-4">{typeof value === 'object' ? JSON.stringify(value) : value.toString()}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    };

    const renderParquetContent = () => {
        console.log('Rendering parquet content with:', { loading, error, parquetData });
        
        if (loading) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>Loading parquet data...</p>
                </div>
            );
        }

        if (error) {
            return (
                <div className="text-center py-8">
                    <div className="p-4 bg-red-900/50 text-red-200 rounded-lg">
                        Error: {error}
                    </div>
                </div>
            );
        }

        if (!parquetData) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>No Parquet data available. Click "View Parquet" to view the data.</p>
                </div>
            );
        }

        // Log the structure of parquetData
        console.log('Parquet data structure:', {
            rowCount: parquetData.rowCount,
            schemaFields: parquetData.schema ? Object.keys(parquetData.schema) : [],
            fullSchema: parquetData.schema
        });

        // Helper function to format field type
        const formatFieldType = (field) => {
            if (typeof field === 'string') return field;
            if (typeof field === 'object') {
                if (field.primitiveType) return field.primitiveType;
                if (field.type) return field.type;
                return JSON.stringify(field);
            }
            return 'unknown';
        };

        // Helper function to check if field is nullable
        const isFieldNullable = (field) => {
            if (typeof field === 'object') {
                if (field.repetitionType === 'OPTIONAL') return true;
                if ('nullable' in field) return field.nullable;
            }
            return true; // Default to true if not specified
        };

        // Helper function to get field description
        const getFieldDescription = (field) => {
            if (typeof field === 'object') {
                if (field.path) return field.path;
                if (field.description) return field.description;
                if (field.originalType) return field.originalType;
            }
            return '-';
        };

        return (
            <div className="space-y-6">
                <div className="grid grid-cols-2 gap-4">
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Total Rows</h3>
                        <p className="text-lg">{parquetData.rowCount?.toLocaleString() || '0'}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Columns</h3>
                        <p className="text-lg">{parquetData.schema ? Object.keys(parquetData.schema).length : '0'}</p>
                    </div>
                </div>
                <div className="bg-[#242424] rounded overflow-hidden">
                    <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Schema</h3>
                    {parquetData.schema && Object.keys(parquetData.schema).length > 0 ? (
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-left text-gray-400 border-b border-gray-800">
                                    <th className="py-3 px-4 font-medium">Field Name</th>
                                    <th className="py-3 px-4 font-medium">Type</th>
                                    <th className="py-3 px-4 font-medium">Nullable</th>
                                    <th className="py-3 px-4 font-medium">Description</th>
                                </tr>
                            </thead>
                            <tbody>
                                {Object.entries(parquetData.schema).map(([fieldName, fieldInfo]) => (
                                    <tr key={fieldName} className="border-b border-gray-800">
                                        <td className="py-2 px-4">{fieldName || '-'}</td>
                                        <td className="py-2 px-4">{formatFieldType(fieldInfo)}</td>
                                        <td className="py-2 px-4">
                                            <span className="bg-[#333333] text-gray-300 px-2 py-0.5 rounded text-xs">
                                                {isFieldNullable(fieldInfo) ? 'Yes' : 'No'}
                                            </span>
                                        </td>
                                        <td className="py-2 px-4 text-gray-400">{getFieldDescription(fieldInfo)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    ) : (
                        <div className="p-4 text-gray-400">
                            No schema information available
                        </div>
                    )}
                </div>
            </div>
        );
    };

    const renderIcebergContent = () => {
        if (loading) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>Loading Iceberg data...</p>
                </div>
            );
        }

        if (error) {
            return (
                <div className="text-center py-8">
                    <div className="p-4 bg-red-900/50 text-red-200 rounded-lg">
                        {error}
                    </div>
                </div>
            );
        }

        if (!icebergData) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>No Iceberg data available. Click "Convert to Iceberg" to view the data.</p>
                </div>
            );
        }

        return (
            <div className="space-y-6">
                <div className="grid grid-cols-3 gap-4">
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Total Records</h3>
                        <p className="text-lg">{icebergData.statistics?.rowCount?.toLocaleString() || '0'}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">File Count</h3>
                        <p className="text-lg">{icebergData.statistics?.fileCount?.toLocaleString() || '0'}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Total Size</h3>
                        <p className="text-lg">{((icebergData.statistics?.totalSize || 0) / (1024 * 1024)).toFixed(2)} MB</p>
                    </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                    <div className="bg-[#242424] rounded overflow-hidden">
                        <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Table Information</h3>
                        <table className="w-full text-sm">
                            <tbody>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Table Name</td>
                                    <td className="py-2 px-4">{icebergData.tableName || '-'}</td>
                                </tr>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Location</td>
                                    <td className="py-2 px-4">{icebergData.location || '-'}</td>
                                </tr>
                                <tr>
                                    <td className="py-2 px-4 text-gray-400">Format</td>
                                    <td className="py-2 px-4">{icebergData.properties?.['write.format.default'] || 'PARQUET'}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="bg-[#242424] rounded overflow-hidden">
                        <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Current Snapshot</h3>
                        <table className="w-full text-sm">
                            <tbody>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Snapshot ID</td>
                                    <td className="py-2 px-4">{icebergData.currentSnapshot?.snapshotId || '-'}</td>
                                </tr>
                                <tr>
                                    <td className="py-2 px-4 text-gray-400">Timestamp</td>
                                    <td className="py-2 px-4">
                                        {icebergData.currentSnapshot?.timestamp 
                                            ? new Date(icebergData.currentSnapshot.timestamp).toLocaleString()
                                            : '-'
                                        }
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="bg-[#242424] rounded overflow-hidden">
                    <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Schema</h3>
                    {icebergData.schema?.fields && icebergData.schema.fields.length > 0 ? (
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-left text-gray-400 border-b border-gray-800">
                                    <th className="py-3 px-4 font-medium">Field Name</th>
                                    <th className="py-3 px-4 font-medium">Type</th>
                                    <th className="py-3 px-4 font-medium">Required</th>
                                    <th className="py-3 px-4 font-medium">ID</th>
                                </tr>
                            </thead>
                            <tbody>
                                {icebergData.schema.fields.map((field, index) => (
                                    <tr key={index} className="border-b border-gray-800">
                                        <td className="py-2 px-4">{field.name || '-'}</td>
                                        <td className="py-2 px-4">{field.type || '-'}</td>
                                        <td className="py-2 px-4">
                                            <span className="bg-[#333333] text-gray-300 px-2 py-0.5 rounded text-xs">
                                                {field.required ? 'Yes' : 'No'}
                                            </span>
                                        </td>
                                        <td className="py-2 px-4">{field.id || '-'}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    ) : (
                        <div className="p-4 text-gray-400">
                            No schema information available
                        </div>
                    )}
                </div>
            </div>
        );
    };

    const renderDeltaContent = () => {
        if (loading) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>Loading delta data...</p>
                </div>
            );
        }

        if (error) {
            return (
                <div className="text-center py-8">
                    <div className="p-4 bg-red-900/50 text-red-200 rounded-lg">
                        {error}
                    </div>
                </div>
            );
        }

        if (!deltaData) {
            return (
                <div className="text-center py-8 text-gray-400">
                    <p>No Delta data available. Click "Convert to Delta" to view the data.</p>
                </div>
            );
        }

        return (
            <div className="space-y-6">
                <div className="grid grid-cols-3 gap-4">
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Total Records</h3>
                        <p className="text-lg">{deltaData.statistics?.numRecords?.toLocaleString()}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">File Count</h3>
                        <p className="text-lg">{deltaData.statistics?.numFiles?.toLocaleString()}</p>
                    </div>
                    <div className="bg-[#242424] p-4 rounded">
                        <h3 className="text-sm font-medium text-gray-400 mb-2">Total Size</h3>
                        <p className="text-lg">{(deltaData.statistics?.totalSize / (1024 * 1024)).toFixed(2)} MB</p>
                    </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                    <div className="bg-[#242424] rounded overflow-hidden">
                        <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Table Information</h3>
                        <table className="w-full text-sm">
                            <tbody>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Table Name</td>
                                    <td className="py-2 px-4">{deltaData.tableName}</td>
                                </tr>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Location</td>
                                    <td className="py-2 px-4">{deltaData.location}</td>
                                </tr>
                                <tr>
                                    <td className="py-2 px-4 text-gray-400">Version</td>
                                    <td className="py-2 px-4">{deltaData.version}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="bg-[#242424] rounded overflow-hidden">
                        <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Protocol</h3>
                        <table className="w-full text-sm">
                            <tbody>
                                <tr className="border-b border-gray-800">
                                    <td className="py-2 px-4 text-gray-400">Min Reader Version</td>
                                    <td className="py-2 px-4">{deltaData.protocol?.minReaderVersion}</td>
                                </tr>
                                <tr>
                                    <td className="py-2 px-4 text-gray-400">Min Writer Version</td>
                                    <td className="py-2 px-4">{deltaData.protocol?.minWriterVersion}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="bg-[#242424] rounded overflow-hidden">
                    <h3 className="text-sm font-medium text-gray-400 p-4 border-b border-gray-800">Files</h3>
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-left text-gray-400 border-b border-gray-800">
                                    <th className="py-3 px-4 font-medium">Path</th>
                                    <th className="py-3 px-4 font-medium">Size</th>
                                    <th className="py-3 px-4 font-medium">Modified</th>
                                </tr>
                            </thead>
                            <tbody>
                                {deltaData.files?.map((file, index) => (
                                    <tr key={index} className="border-b border-gray-800">
                                        <td className="py-2 px-4">{file.path}</td>
                                        <td className="py-2 px-4">{(file.size / (1024 * 1024)).toFixed(2)} MB</td>
                                        <td className="py-2 px-4">{new Date(file.modificationTime).toLocaleString()}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        );
    };

    const StatCard = ({ title, value, icon, color = 'blue' }) => (
        <div className="bg-white rounded-xl shadow-lg p-6 transform transition-all duration-300 hover:scale-105 hover:shadow-xl">
            <div className="flex items-center">
                <div className={`p-3 rounded-full bg-${color}-100 text-${color}-600`}>
                    {icon}
                </div>
                <div className="ml-4">
                    <p className="text-sm font-medium text-gray-500">{title}</p>
                    <p className="text-2xl font-bold text-gray-900">{value}</p>
                </div>
            </div>
        </div>
    );

    const renderSidebar = () => (
        <div className="w-[200px] min-w-[200px] bg-[#141414] h-screen flex flex-col">
            <div className="p-4 border-b border-gray-800">
                <h1 className="text-xl font-medium">Meta Lake</h1>
            </div>
            <div className="p-4 space-y-2">
                {/* Main Navigation */}
                <div className="space-y-1">
                    <button 
                        onClick={() => setCurrentPage('home')}
                        className={`w-full text-left px-4 py-2 rounded text-sm flex items-center gap-2 ${
                            currentPage === 'home' 
                                ? 'bg-blue-500 text-white' 
                                : 'text-gray-300 hover:bg-[#1a1a1a]'
                        }`}
                    >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
                        </svg>
                        Home
                    </button>
                    <button 
                        onClick={() => {
                            setCurrentPage('parquet-minio');
                            setViewerType('minio');
                        }}
                        className={`w-full text-left px-4 py-2 rounded text-sm flex items-center gap-2 ${
                            currentPage === 'parquet-minio' && viewerType === 'minio'
                                ? 'bg-blue-500 text-white' 
                                : 'text-gray-300 hover:bg-[#1a1a1a]'
                        }`}
                    >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7z" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h8" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 16V8" />
                        </svg>
                        Parquet Viewer (MinIO)
                    </button>
                    <button 
                        onClick={() => {
                            setCurrentPage('parquet-minio');
                            setViewerType('aws');
                        }}
                        className={`w-full text-left px-4 py-2 rounded text-sm flex items-center gap-2 ${
                            currentPage === 'parquet-minio' && viewerType === 'aws'
                                ? 'bg-blue-500 text-white' 
                                : 'text-gray-300 hover:bg-[#1a1a1a]'
                        }`}
                    >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7z" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h8" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 16V8" />
                        </svg>
                        Parquet Viewer (AWS)
                    </button>
                </div>

                {/* Show these only when in Parquet Viewer */}
                {currentPage === 'parquet-minio' && (
                    <div className="mt-6 space-y-1">
                        <div className="text-xs text-gray-500 uppercase px-4 py-2">Explorer</div>
                        <button 
                            onClick={() => handleTabChange('buckets')}
                            className={`w-full text-left px-4 py-2 rounded text-sm ${
                                activeTab === 'buckets' 
                                    ? 'bg-[#1a1a1a] text-blue-400' 
                                    : 'text-gray-300 hover:bg-[#1a1a1a]'
                            }`}
                        >
                            Buckets
                        </button>
                        <button 
                            onClick={() => handleTabChange('tables')}
                            className={`w-full text-left px-4 py-2 rounded text-sm ${
                                activeTab === 'tables' 
                                    ? 'bg-[#1a1a1a] text-blue-400' 
                                    : 'text-gray-300 hover:bg-[#1a1a1a]'
                            }`}
                        >
                            Tables
                        </button>
                    </div>
                )}
            </div>
        </div>
    );

    return (
        <div className="min-h-screen bg-[#1a1a1a] text-white flex">
            {renderSidebar()}
            
            {/* Main Content */}
            {currentPage === 'parquet-minio' ? (
                <div className="flex-1 overflow-auto">
                    <div className="p-6 space-y-6">
                        {/* Input Section */}
                        <div className="bg-[#1e1e1e] rounded-lg p-6">
                            <div className="flex justify-between items-center mb-4">
                                <h2 className="text-lg font-medium">{viewerType === 'minio' ? 'MinIO' : 'AWS'} Connection Details</h2>
                                <div className="px-3 py-1 bg-blue-500/10 text-blue-400 rounded text-sm">
                                    Connected to {viewerType === 'minio' ? 'MinIO' : 'AWS'}
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-4 mb-4">
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Bucket Name</label>
                                    <input
                                        type="text"
                                        value={bucketName}
                                        onChange={(e) => setBucketName(e.target.value)}
                                        placeholder="Enter bucket name"
                                        className="w-full px-3 py-2 bg-[#242424] border border-gray-800 rounded focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">File Key</label>
                                    <input
                                        type="text"
                                        value={fileKey}
                                        onChange={(e) => setFileKey(e.target.value)}
                                        placeholder="Enter file key"
                                        className="w-full px-3 py-2 bg-[#242424] border border-gray-800 rounded focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                                    />
                                </div>
                            </div>
                            <div className="flex space-x-3">
                                {/* Action buttons */}
                            <button
                                onClick={handleFetchMetadata}
                                disabled={loading}
                                    className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:opacity-50 text-sm"
                                >
                                    {loading ? 'Loading...' : 'Fetch Metadata'}
                                </button>
                                <button
                                    onClick={handleViewParquet}
                                    disabled={loading}
                                    className="px-4 py-2 bg-green-500 text-white rounded hover:bg-green-600 disabled:opacity-50 text-sm"
                                >
                                    {loading ? 'Loading...' : 'View Parquet'}
                                </button>
                                <button
                                    onClick={handleConvertToIceberg}
                                    disabled={loading}
                                    className="px-4 py-2 bg-purple-500 text-white rounded hover:bg-purple-600 disabled:opacity-50 text-sm"
                                >
                                    {loading ? 'Converting...' : 'Convert to Iceberg'}
                                </button>
                                <button
                                    onClick={handleConvertToDelta}
                                    disabled={loading}
                                    className="px-4 py-2 bg-indigo-500 text-white rounded hover:bg-indigo-600 disabled:opacity-50 text-sm"
                                >
                                    {loading ? 'Converting...' : 'Convert to Delta'}
                                </button>
                            </div>
                        </div>

                        {/* Rest of your existing viewer content */}
                        <div className="bg-[#1e1e1e] rounded-lg">
                            {/* Table Details */}
                            <div className="p-6 border-b border-gray-800">
                                <h2 className="text-lg font-medium mb-4">Table Details</h2>
                                <div className="grid grid-cols-4 gap-4">
                                    <div className="bg-[#242424] p-4 rounded">
                                        <p className="text-gray-400 text-sm mb-1">Format</p>
                                        <p className="text-sm">PARQUET</p>
                                    </div>
                                    <div className="bg-[#242424] p-4 rounded">
                                        <p className="text-gray-400 text-sm mb-1">Files</p>
                                        <p className="text-sm">{metadata?.files || '2'}</p>
                                    </div>
                                    <div className="bg-[#242424] p-4 rounded">
                                        <p className="text-gray-400 text-sm mb-1">Total Size</p>
                                        <p className="text-sm">{metadata?.size || '21.3 KB'}</p>
                                    </div>
                                    <div className="bg-[#242424] p-4 rounded">
                                        <p className="text-gray-400 text-sm mb-1">Last Modified</p>
                                        <p className="text-sm">{metadata?.lastModified || '3/30/2025, 8:50:28 AM'}</p>
                                    </div>
                                </div>
                            </div>

                            {/* Tabs and Content */}
                            <div>
                                {/* Tabs */}
                                <div className="flex border-b border-gray-800 px-6">
                                    <button
                                        className={`py-3 px-4 text-sm font-medium ${activeTab === 'metadata' ? 'text-blue-400 border-b-2 border-blue-400 -mb-[2px]' : 'text-gray-400'}`}
                                        onClick={() => handleTabChange('metadata')}
                                    >
                                        METADATA
                                    </button>
                                    <button
                                        className={`py-3 px-4 text-sm font-medium ${activeTab === 'parquet' ? 'text-blue-400 border-b-2 border-blue-400 -mb-[2px]' : 'text-gray-400'}`}
                                        onClick={() => handleTabChange('parquet')}
                                    >
                                        PARQUET
                                    </button>
                                    <button
                                        className={`py-3 px-4 text-sm font-medium ${activeTab === 'iceberg' ? 'text-blue-400 border-b-2 border-blue-400 -mb-[2px]' : 'text-gray-400'}`}
                                        onClick={() => handleTabChange('iceberg')}
                                    >
                                        ICEBERG
                                    </button>
                                    <button
                                        className={`py-3 px-4 text-sm font-medium ${activeTab === 'delta' ? 'text-blue-400 border-b-2 border-blue-400 -mb-[2px]' : 'text-gray-400'}`}
                                        onClick={() => handleTabChange('delta')}
                                    >
                                        DELTA
                                    </button>
                                </div>

                                {/* Content */}
                                <div className="p-6">
                                    {activeTab === 'metadata' && renderMetadataContent()}
                                    {activeTab === 'parquet' && renderParquetContent()}
                                    {activeTab === 'iceberg' && renderIcebergContent()}
                                    {activeTab === 'delta' && renderDeltaContent()}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="flex-1 overflow-auto p-6">
                    <div className="max-w-4xl mx-auto">
                        <h1 className="text-3xl font-bold mb-6">Welcome to Meta Lake</h1>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div className="bg-[#1e1e1e] p-6 rounded-lg">
                                <h2 className="text-xl font-medium mb-4">Parquet Viewer (MinIO)</h2>
                                <p className="text-gray-400 mb-4">
                                    View and analyze Parquet files stored in MinIO buckets. Convert them to Iceberg or Delta format.
                                </p>
                                <button 
                                    onClick={() => setCurrentPage('parquet-minio')}
                                    className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                                >
                                    Open Viewer
                                </button>
                            </div>
                            <div className="bg-[#1e1e1e] p-6 rounded-lg opacity-100">
                                <div className="absolute top-2 right-2 px-2 py-1 bg-blue-500/10 text-blue-400 rounded text-xs">
                                    bhokat
                                </div>
                                <h2 className="text-xl font-medium mb-4">Parquet Viewer (AWS)</h2>
                                <p className="text-gray-400 mb-4">
                                    View and analyze Parquet files stored in AWS S3 buckets. Connect directly to your AWS infrastructure.
                                </p>
                                <button 
                                    onClick={() => window.location.href = 'http://localhost:5173/'}
                                    className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                                >
                                    Open Viewer
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default App;
