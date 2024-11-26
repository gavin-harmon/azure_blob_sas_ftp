# Azure Blob Storage Explorer

A streamlined web application built with Streamlit for exploring and managing Azure Blob Storage containers. This tool provides an intuitive interface for common file operations and blob storage management.

## Features

- 🔐 Secure authentication with SAS tokens
- 📁 Browse blob containers and directories
- ⬆️ Upload files (with drag & drop support)
- ⬇️ Download files
- 🗑️ Delete files and directories
- 📂 Directory navigation
- 🔍 File metadata display
- 💫 Clean, modern interface

## Prerequisites

- Python 3.7+
- A valid Azure Storage Account
- SAS token with appropriate permissions (read, write, list, delete)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/azure-blob-explorer.git
cd azure-blob-explorer
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Running the Application

Start the application using Streamlit:
```bash
streamlit run app.py
```

## Usage

1. Provide your Azure credentials in the sidebar:
   - Storage Account Name
   - Container Name
   - SAS Token

2. Click "Connect" to access your storage container

3. Use the interface to:
   - Navigate through directories
   - Upload files using the drag & drop interface
   - Download files using the download buttons
   - Delete files or directories with confirmation

## Security Note

- Credentials are only stored in session state
- No persistent storage of authentication details
- SAS tokens are masked in the interface
- Session data is cleared on disconnect

## Requirements

```
streamlit
azure-storage-blob
python-dateutil
posixpath
```

## Deployment

This application can be deployed on Streamlit Cloud or any other Python hosting service that supports Streamlit applications.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License
