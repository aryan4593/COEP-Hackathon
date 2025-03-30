# Data Metadata Viewer

A modern web application for viewing and analyzing metadata from various data sources, supporting both AWS S3 and MinIO storage systems. This project provides a unified interface for exploring and managing data metadata across different storage solutions.

## Project Overview

This project consists of three main components:

1. **Frontend Application** (`FRONT-END/`)
   - Built with Next.js and TypeScript
   - Modern UI with Tailwind CSS and Radix UI components
   - 3D visualization capabilities using Three.js
   - Responsive and interactive design

2. **AWS Metadata Service** (`AWS-metadata/`)
   - Backend service for AWS S3 integration
   - FastAPI-based API endpoints
   - Support for Parquet, Iceberg, and Delta table formats
   - Comprehensive metadata analysis capabilities

3. **MinIO Metadata Service** (`minIO-metadata/`)
   - Backend service for MinIO integration
   - Similar capabilities to AWS service
   - Local storage support
   - Compatible with S3 API

## Features

- **Unified Interface**: Single platform for managing metadata across different storage systems
- **Advanced Visualization**: 3D data visualization and interactive charts
- **Schema Analysis**: Detailed view of table schemas and data types
- **Snapshot Management**: Compare and analyze table snapshots
- **Modern UI Components**: Built with Radix UI and Tailwind CSS
- **Responsive Design**: Works seamlessly across different devices
- **Real-time Updates**: Live data refresh and monitoring

## Tech Stack

### Frontend
- Next.js 15.2.4
- React 19
- TypeScript
- Tailwind CSS
- Radix UI Components
- Three.js for 3D visualization

### Backend
- FastAPI
- Python
- AWS SDK
- MinIO Client
- Parquet/Delta/Iceberg support

## Getting Started

### Prerequisites
- Node.js (Latest LTS version)
- Python 3.8+
- AWS Account (for AWS S3 access)
- MinIO Server (for local development)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <project-directory>
```

2. Frontend Setup:
```bash
cd FRONT-END
npm install
npm run dev
```

3. AWS Metadata Service Setup:
```bash
cd AWS-metadata
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn backend.app.main:app --reload
```

4. MinIO Metadata Service Setup:
```bash
cd minIO-metadata
npm install
```

### Environment Configuration

Create `.env` files in the respective directories:

#### AWS Service
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

#### MinIO Service
```
MINIO_ENDPOINT=your_minio_endpoint
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
```

## Usage

1. Start all services (Frontend, AWS Service, and MinIO Service)
2. Access the application at `http://localhost:3000`
3. Choose your storage system (AWS S3 or MinIO)
4. Browse and analyze your data metadata

## Project Structure

```
project/
├── FRONT-END/           # Next.js frontend application
│   ├── src/            # Source code
│   ├── public/         # Static assets
│   └── package.json    # Frontend dependencies
├── AWS-metadata/       # AWS S3 metadata service
│   ├── backend/       # FastAPI backend
│   └── frontend/      # AWS-specific frontend
└── minIO-metadata/    # MinIO metadata service
    └── meta1/         # MinIO-specific implementation
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- COEP Hackathon Team
- All contributors and maintainers
- Open-source community for the amazing tools and libraries used in this project