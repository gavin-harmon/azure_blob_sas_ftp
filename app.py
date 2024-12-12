import streamlit as st
import os
from azure.storage.blob import BlobServiceClient, BlobPrefix
from datetime import datetime
import posixpath



# Page configuration
st.set_page_config(
    page_title="Azure Blob Storage Explorer",
    layout="wide"
)

# Initialize session state
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'blob_service_client' not in st.session_state:
    st.session_state.blob_service_client = None
if 'container_client' not in st.session_state:
    st.session_state.container_client = None
if 'current_path' not in st.session_state:
    st.session_state.current_path = ''
if 'show_welcome' not in st.session_state:
    st.session_state.show_welcome = True

# Custom styling
# Custom styling
st.markdown("""
    <style>
    /* Base app styling */
    .stApp {
        background-color: #f8f9fa;
    }

    /* Remove extra spaces and default padding */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Navigation styling */
    .navigation-bar {
        padding: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
    }

    .current-path {
        color: #6c757d;
        padding: 0.5rem;
        background-color: #f8f9fa;
        border-radius: 4px;
        margin-left: 1rem;
    }

    /* File browser styling */
    .file-list-header {
        color: #6c757d;
        font-size: 0.875rem;
        padding: 0.5rem;
        border-bottom: 1px solid #e9ecef;
    }

    .file-row {
        padding: 0.5rem;
        margin: 0;
        line-height: 1.5;
        display: flex;
        align-items: center;
    }

    .file-row:hover {
        background-color: #f8f9fa;
    }

    /* Button styling */
    .stButton button {
        width: 100%;
        text-align: left;
        padding: 0.5rem !important;
        line-height: 1.5;
        border: none;
        background: none;
        margin: 0 !important;
    }

    .stButton button:hover {
        background-color: #f8f9fa;
    }

    /* Action buttons */
    .action-button {
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        cursor: pointer;
        transition: background-color 0.2s;
    }

    .action-button:hover {
        background-color: #f8f9fa;
    }

    .delete-button {
        color: #dc3545;
    }

    /* Upload section styling */
    .upload-section {
        padding: 1rem;
        margin-bottom: 1rem;
    }

    /* Breadcrumb styling */
    .breadcrumb {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem;
        border-radius: 4px;
    }

    .breadcrumb-item {
        color: #0d6efd;
        text-decoration: none;
        cursor: pointer;
    }

    .breadcrumb-separator {
        color: #6c757d;
    }

    /* File/Folder icons and text */
    .file-name {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }

    .folder-name {
        color: #1f77b4;
        font-weight: 500;
    }

    /* Status messages */
    .success-message {
        color: #28a745;
        padding: 0.5rem;
        margin: 0.5rem 0;
        border-radius: 4px;
    }

    .error-message {
        color: #dc3545;
        padding: 0.5rem;
        margin: 0.5rem 0;
        border-radius: 4px;
    }

    /* Responsive adjustments */
    @media (max-width: 768px) {
        .file-row {
            flex-direction: column;
            align-items: flex-start;
        }

        .action-button {
            margin-top: 0.5rem;
        }
    }
    </style>
""", unsafe_allow_html=True)


def validate_container_access(account_name, container_name, sas_token):
    """Validate Azure credentials by attempting to list blobs in the container"""
    try:
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)
        next(container_client.list_blobs(), None)
        return blob_service_client, container_client
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None, None


def get_directory_contents(container_client, prefix=''):
    """Get contents of current directory, properly handling the virtual directory structure"""
    try:
        directories = set()
        files = []

        # Normalize prefix
        prefix = prefix if prefix.endswith('/') or prefix == '' else prefix + '/'

        # Use include=['metadata', 'timestamps'] to only fetch necessary properties
        # This significantly reduces data transfer and improves performance
        blobs = container_client.list_blobs(
            name_starts_with=prefix,
            include=['metadata']
        )

        # Process the listing stream
        for blob in blobs:
            relative_path = blob.name[len(prefix):] if prefix else blob.name
            if not relative_path:
                continue

            # Handle directories
            if '/' in relative_path:
                dir_name = relative_path.split('/')[0]
                dir_path = prefix + dir_name + '/'
                if dir_path not in directories:
                    directories.add(dir_path)
                continue

            # Add files (only in current directory level)
            files.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified
            })

        # Convert to list format
        dir_list = [{'name': d, 'is_directory': True} for d in sorted(directories)]
        file_list = [{**f, 'is_directory': False} for f in sorted(files, key=lambda x: x['name'])]

        return dir_list + file_list

    except Exception as e:
        st.error(f"Error listing contents: {str(e)}")
        return []

def download_blob(container_client, blob_name):
    """Download a blob from Azure Storage with improved error handling and progress"""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        
        # First get blob properties to check size
        properties = blob_client.get_blob_properties()
        size_mb = properties.size / (1024 * 1024)
        
        # Add a warning for large files
        if size_mb > 100:  # Warning for files larger than 100MB
            st.warning(f"Large file detected ({size_mb:.1f}MB). Download may take some time.")
        
        # Create a progress bar for larger files
        if size_mb > 10:  # Only show progress for files larger than 10MB
            progress_bar = st.progress(0)
            
            # Download in chunks for large files
            chunk_size = 1024 * 1024  # 1MB chunks
            total_chunks = int(properties.size / chunk_size) + 1
            
            # Download stream
            download_stream = blob_client.download_blob()
            data = bytearray()
            
            for i, chunk in enumerate(download_stream.chunks()):
                data.extend(chunk)
                if progress_bar is not None:
                    progress_bar.progress(min((i + 1) / total_chunks, 1.0))
            
            if progress_bar is not None:
                progress_bar.empty()
            
            return bytes(data)
        else:
            # For smaller files, download directly
            blob_data = blob_client.download_blob()
            return blob_data.readall()
            
    except Exception as e:
        st.error(f"Error downloading file: {str(e)}")
        return None

def upload_files(container_client, files, current_path):
    """Upload multiple files to Azure Blob Storage"""
    try:
        for file in files:
            blob_name = posixpath.join(current_path, file.name).lstrip('/')
            container_client.upload_blob(name=blob_name, data=file, overwrite=True)
            st.success(f"Successfully uploaded {file.name}")
    except Exception as e:
        st.error(f"Error uploading files: {str(e)}")


def download_blob(container_client, blob_name):
    """Download a blob from Azure Storage"""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob()
        return blob_data.readall()
    except Exception as e:
        st.error(f"Error downloading file: {str(e)}")
        return None


def show_navigation():
    """Display the navigation bar with path and controls"""
    st.markdown('<div class="navigation-bar">', unsafe_allow_html=True)
    cols = st.columns([1, 6, 1])  # Added a column for refresh button

    # Back button
    with cols[0]:
        if st.session_state.current_path:
            if st.button("← Back"):
                path_parts = st.session_state.current_path.rstrip('/').split('/')
                st.session_state.current_path = '/'.join(path_parts[:-1])
                if st.session_state.current_path:
                    st.session_state.current_path += '/'
                st.rerun()

    # Current path display
    with cols[1]:
        if st.session_state.current_path:
            st.markdown(f'<div class="current-path">/{st.session_state.current_path}</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="current-path">/</div>', unsafe_allow_html=True)

    # Refresh button
    with cols[2]:
        if st.button("🔄 Refresh"):
            st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

    # Current path display
    with cols[1]:
        if st.session_state.current_path:
            st.markdown(f'<div class="current-path">/{st.session_state.current_path}</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="current-path">/</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


def show_welcome_screen():
    """Display welcome screen with instructions"""
    st.header("Welcome to Azure Blob Storage Explorer")
    st.markdown("""
    ### Getting Started
    1. Connect to your Azure Storage account using the sidebar on the left
    2. You'll need:
        - Storage Account Name
        - Container Name
        - SAS Token (Shared Access Signature)

    ### Features
    - Browse files and folders
    - Upload files to any location
    - Download files
    - Navigate through directories

    ### Security Note
    Your connection credentials are not stored and are only used for the current session.
    """)


def show_sidebar():
    """Display sidebar with connection controls"""
    with st.sidebar:
        st.header("Connection Settings")
        account_name = st.text_input("Storage Account Name")
        container_name = st.text_input("Container Name")
        sas_token = st.text_input("SAS Token", type="password")

        if st.button("Connect" if not st.session_state.connected else "Disconnect"):
            if not st.session_state.connected:
                if account_name and container_name and sas_token:
                    blob_service_client, container_client = validate_container_access(
                        account_name, container_name, sas_token)
                    if blob_service_client and container_client:
                        st.session_state.blob_service_client = blob_service_client
                        st.session_state.container_client = container_client
                        st.session_state.connected = True
                        st.session_state.show_welcome = False
                        st.rerun()
                else:
                    st.error("Please provide all connection details")
            else:
                st.session_state.blob_service_client = None
                st.session_state.container_client = None
                st.session_state.connected = False
                st.session_state.current_path = ''
                st.session_state.show_welcome = True
                st.rerun()

        st.write("Status:", "🟢 Connected" if st.session_state.connected else "🔴 Disconnected")


def delete_blob(container_client, blob_name):
    """Delete a blob from Azure Storage"""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.delete_blob()
        return True
    except Exception as e:
        st.error(f"Error deleting file: {str(e)}")
        return False

def delete_directory(container_client, directory_path):
    """Delete all blobs within a directory"""
    try:
        # List all blobs in directory
        blobs = container_client.list_blobs(name_starts_with=directory_path)
        for blob in blobs:
            container_client.delete_blob(blob.name)
        return True
    except Exception as e:
        st.error(f"Error deleting directory: {str(e)}")
        return False




def show_file_browser():
    """Display the file browser interface"""
    st.title("Azure Blob Storage Explorer")

    # Show navigation
    show_navigation()

    # File browser section
    st.markdown('<div class="file-browser">', unsafe_allow_html=True)

    # List contents
    items = get_directory_contents(st.session_state.container_client, st.session_state.current_path)

    if items:
        # Headers
        cols = st.columns([3, 2, 2, 1])
        cols[0].markdown('<div class="file-list-header">Name</div>', unsafe_allow_html=True)
        cols[1].markdown('<div class="file-list-header">Size</div>', unsafe_allow_html=True)
        cols[2].markdown('<div class="file-list-header">Last Modified</div>', unsafe_allow_html=True)
        cols[3].markdown('<div class="file-list-header">Actions</div>', unsafe_allow_html=True)

        for item in items:
            display_name = item['name'].rstrip('/').split('/')[-1]
            cols = st.columns([3, 2, 2, 1])

            # Name column
            with cols[0]:
                if item['is_directory']:
                    if st.button(f"📁 {display_name}", key=f"dir_{item['name']}"):
                        st.session_state.current_path = item['name']
                        st.rerun()
                else:
                    st.markdown(f"<div class='file-row'>📄 {display_name}</div>", unsafe_allow_html=True)

            # Size and modification date
            cols[1].markdown(f'<div class="file-row">{format_size(item.get("size", None))}</div>',
                             unsafe_allow_html=True)
            cols[2].markdown(
                f'<div class="file-row">{item.get("last_modified", "-").strftime("%Y-%m-%d %H:%M:%S") if item.get("last_modified") else "-"}</div>',
                unsafe_allow_html=True
            )

            # Actions column
            with cols[3]:
                action_cols = st.columns([1, 1])
                if not item['is_directory']:
                    # Download button
                    with action_cols[0]:
                        if st.button("⬇️", key=f"download_btn_{item['name']}"):
                            # Only download when button is clicked
                            with st.spinner('Downloading...'):
                                blob_data = download_blob(st.session_state.container_client, item['name'])
                                if blob_data:
                                    # Use st.download_button only after user initiates download
                                    st.download_button(
                                        label="Save File",
                                        data=blob_data,
                                        file_name=display_name,
                                        key=f"save_{item['name']}"
                                    )

                # Delete button
                with action_cols[1]:
                    if st.button("🗑️", key=f"delete_{item['name']}",
                                 help="Delete" + (" directory" if item['is_directory'] else " file")):
                        if st.session_state.get(f"confirm_delete_{item['name']}", False):
                            # Perform deletion
                            if item['is_directory']:
                                if delete_directory(st.session_state.container_client, item['name']):
                                    st.success(f"Directory {display_name} deleted successfully")
                                    st.rerun()
                            else:
                                if delete_blob(st.session_state.container_client, item['name']):
                                    st.success(f"File {display_name} deleted successfully")
                                    st.rerun()
                        else:
                            # Show confirmation
                            st.session_state[f"confirm_delete_{item['name']}"] = True
                            st.warning(
                                f"You sure?")

    # Upload section
    st.markdown('<div class="upload-section">', unsafe_allow_html=True)
    st.markdown(
        f"**Current upload location:** {'/' + st.session_state.current_path if st.session_state.current_path else 'Root'}")
    uploaded_files = st.file_uploader("Drop files here", accept_multiple_files=True)
    if uploaded_files:
        upload_files(st.session_state.container_client, uploaded_files, st.session_state.current_path)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)




def main():
    show_sidebar()
    if st.session_state.show_welcome and not st.session_state.connected:
        show_welcome_screen()
    elif st.session_state.connected:
        show_file_browser()


if __name__ == "__main__":
    main()
