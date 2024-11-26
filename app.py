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
st.markdown("""
    <style>
    /* Sidebar specific styles */
    .css-1d391kg {  /* Sidebar */
        background-color: #1e1e1e;
    }

    .stButton button {
        background-color: #2d2d2d !important;
        color: #ffffff !important;
        border: 1px solid #404040 !important;
    }

    .stButton button:hover {
        background-color: #404040 !important;
        border-color: #565656 !important;
    }

    /* Status indicator */
    [data-testid="stMarkdownContainer"] {
        color: #ffffff !important;
    }

    /* File/Folder icons and text */
    .file-row {
        color: #e0e0e0 !important;
        background-color: transparent !important;
    }

    /* Folder icon specific styling */
    button[key^="dir_"] {
        background-color: transparent !important;
        color: #00a2ff !important;
    }

    /* Empty cell styling */
    .empty-cell {
        color: #666666 !important;
    }

    /* Main content area */
    .stApp {
        background-color: #2b2b2b;
    }

    .file-list-header {
        color: #ffffff !important;
        background-color: #1e1e1e;
        border-bottom: 1px solid #404040;
    }

    /* Upload section */
    [data-testid="stFileUploadDropzone"] {
        background-color: #1e1e1e !important;
        border-color: #404040 !important;
        color: #ffffff !important;
    }

    /* Input fields */
    .stTextInput > div > div > input {
        color: #ffffff !important;
        background-color: #1e1e1e !important;
        border-color: #404040 !important;
    }

    /* Action buttons */
    .stDownloadButton button {
        background-color: transparent !important;
        color: #00a2ff !important;
        border: none !important;
    }

    .stDownloadButton button:hover {
        background-color: #404040 !important;
    }

    /* Navigation */
    .current-path {
        color: #e0e0e0 !important;
        background-color: #1e1e1e !important;
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

        # List all blobs with the prefix
        blobs = list(container_client.list_blobs(name_starts_with=prefix))

        # First pass: identify directories
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

        # Second pass: add files that are not directories
        for blob in blobs:
            # Skip if this blob's path is in our directories set
            if blob.name + '/' in directories or blob.name in directories:
                continue

            # Only include files in the current directory level
            relative_path = blob.name[len(prefix):] if prefix else blob.name
            if not relative_path or '/' in relative_path:
                continue

            files.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified
            })

        # Convert to list format
        dir_list = [{'name': d, 'is_directory': True} for d in directories]
        file_list = [{**f, 'is_directory': False} for f in files]

        return sorted(dir_list + file_list, key=lambda x: (not x['is_directory'], x['name']))
    except Exception as e:
        st.error(f"Error listing contents: {str(e)}")
        return []


def format_size(size_in_bytes):
    """Format file size to human readable format"""
    if size_in_bytes is None:
        return "-"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.1f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.1f} PB"


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
    cols = st.columns([1, 6])

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
                        blob_data = download_blob(st.session_state.container_client, item['name'])
                        if blob_data:
                            st.download_button(
                                label="⬇️",
                                data=blob_data,
                                file_name=display_name,
                                key=f"download_{item['name']}"
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
                                f"Are you sure you want to delete {display_name}? Click delete button again to confirm.")

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
