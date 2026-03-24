from __future__ import annotations
import io
import os
from pathlib import Path
import sys
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

import sys
from pathlib import Path

if getattr(sys, 'frozen', False):
    SCRIPT_DIR = Path(sys.executable).resolve().parent
else:
    SCRIPT_DIR = Path(__file__).resolve().parent
SCOPES = ["https://www.googleapis.com/auth/drive"]
DIRECT = "https://drive.google.com/uc?export=download&id="

IDs = {}
CHILDREN = {}
TRACE_API_CALLS = False

FILE_LOCATIONS = json.loads((SCRIPT_DIR / "file_locations.json").read_text())

# ------------------------------------------------------------
# AUTHENTICATION
# ------------------------------------------------------------

def auth():
    token_path = SCRIPT_DIR / "token.json"
    a = input("Would you like to use a public account? (Y/N):") 
    if a == "Y":
        with open(SCRIPT_DIR / "public_token.json", "rb") as src, open(token_path, "wb") as dst:
            dst.write(src.read())
    elif Path.exists(SCRIPT_DIR / "private_token.json"):
            with open(SCRIPT_DIR / "private_token.json", "rb") as src, open(SCRIPT_DIR / "token.json", "wb") as dst:
                dst.write(src.read())
    secret_path = SCRIPT_DIR / "client_secret.json"

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(secret_path, SCOPES)
        creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    if (a == "Y"):
        os.remove(token_path)
    return build("drive", "v3", credentials=creds)


if (TRACE_API_CALLS):
    orig_files = drive.files  # this is a callable that returns a FilesResource

    class FilesWrapper:
        def __init__(self, real):
            self._real = real

        def list(self, *a, **kw):
            print("Drive API CALL: files.list")
            print("  kwargs:", kw)
            return self._real.list(*a, **kw)

        def create(self, *a, **kw):
            print("Drive API CALL: files.create")
            print("  kwargs:", kw)
            return self._real.create(*a, **kw)

        def update(self, *a, **kw):
            print("Drive API CALL: files.update")
            print("  kwargs:", kw)
            return self._real.update(*a, **kw)

        def get(self, *a, **kw):
            print("Drive API CALL: files.get")
            print("  kwargs:", kw)
            return self._real.get(*a, **kw)

        def get_media(self, *a, **kw):
            print("Drive API CALL: files.get_media")
            print("  kwargs:", kw)
            return self._real.get_media(*a, **kw)
    def wrapped_files():
        return FilesWrapper(orig_files())

    drive.files = wrapped_files
# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------

def is_drive_path(s) -> bool:
    return str(s).startswith("drive:/")

def drive_path_to_components(path: str):
    """drive:/a/b/c.bin → ['a','b','c.bin']"""
    return path[len("drive:/"):].strip("/").split("/")

def find_child(parent_id: str, name: str):
    """Find a file/folder named `name` inside folder `parent_id`, with caching."""

    key = (parent_id, name)

    # Fast path: cached positive result
    if key in CHILDREN:
        return CHILDREN[key]

    # Escape single quotes for Drive query
    safe_name = name.replace("'", "\\'")

    q = (
        f"'{parent_id}' in parents and "
        f"name = '{safe_name}' and trashed = false"
    )

    res = drive.files().list(
        q=q,
        fields="files(id, name, mimeType)"
    ).execute()

    files = res.get("files", [])
    child = files[0] if files else None

    if child is not None:
        CHILDREN[key] = child

    return child
def resolve_drive_path(path: str) -> str | None:
    """
    Resolve drive:/a/b/c.bin → file ID.
    Creates intermediate folders if needed.
    Returns None if the final file does not exist.
    """

    # Fast path: full path already cached
    if path in IDs:
        return IDs[path]

    parts = drive_path_to_components(path)
    parent = "root"
    prefix = "drive:/"

    for i, part in enumerate(parts):
        prefix = prefix + part if prefix == "drive:/" else prefix + "/" + part

        # Cached prefix?
        if prefix in IDs:
            parent = IDs[prefix]
            continue

        # Not cached → check Drive
        existing = find_child(parent, part)

        if existing:
            parent = existing["id"]
        else:
            if i < len(parts) - 1:
                # Create intermediate folder
                meta = {
                    "name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent]
                }
                folder = drive.files().create(body=meta, fields="id").execute()
                parent = folder["id"]
            else:
                # Final file does not exist
                return None

        # Cache this prefix
        IDs[prefix] = parent

    # Cache full path
    IDs[path] = parent
    return parent
# ------------------------------------------------------------
# CORE FUNCTIONS (ID ONLY)
# ------------------------------------------------------------

def read(path_or_id: str) -> bytes:
    """
    Reads a file from Drive.
    Accepts:
      - drive:/path/to/file
      - raw file ID
    Returns raw bytes.
    """
    if is_drive_path(path_or_id):
        file_id = resolve_drive_path(path_or_id)
        if not file_id:
            raise FileNotFoundError(path_or_id)
    else:
        file_id = path_or_id  # already an ID

    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return buf.getvalue().decode("utf-8")


def create(local_path: str, drive_path: str | None = None) -> str:
    """
    Dual‑mode create():
      1) create(local_path, drive_path):
            Uploads a local file to a virtual Drive path.
            Overwrites if the file already exists.
            Returns the file ID.

      2) create(drive_path):
            Creates the folder chain described by drive_path.
            Returns the final folder ID.
    """

    # ------------------------------------------------------------
    # MODE 2: Folder creation (only one argument passed)
    # ------------------------------------------------------------
    if drive_path is None:
        drive_path = local_path  # reinterpret argument

        if not is_drive_path(drive_path):
            raise ValueError("Folder creation requires a drive:/ path")

        parts = drive_path_to_components(drive_path)
        parent = "root"

        for part in parts:
            existing = find_child(parent, part)
            if existing:
                parent = existing["id"]
            else:
                meta = {
                    "name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent]
                }
                folder = drive.files().create(body=meta, fields="id").execute()
                parent = folder["id"]

        return parent  # final folder ID


    # ------------------------------------------------------------
    # MODE 1: File upload (two arguments passed)
    # ------------------------------------------------------------
    if not is_drive_path(drive_path):
        raise ValueError("drive_path must start with drive:/")

    parts = drive_path_to_components(drive_path)
    filename = parts[-1]
    folder_path = "drive:/" + "/".join(parts[:-1])

    # Ensure folder exists
    folder_id = resolve_drive_path(folder_path)
    if folder_id is None:
        resolve_drive_path(folder_path + "/__dummy__")
        folder_id = resolve_drive_path(folder_path)

    # Check if file already exists
    existing = find_child(folder_id, filename)

    media = MediaIoBaseUpload(
        open(local_path, "rb"),
        mimetype="application/octet-stream"
    )

    if existing:
        # Overwrite existing file
        file = drive.files().update(
            fileId=existing["id"],
            media_body=media,
            fields="id"
        ).execute()
        return file["id"]

    # Otherwise create a new file
    meta = {"name": filename, "parents": [folder_id]}
    file = drive.files().create(
        body=meta,
        media_body=media,
        fields="id"
    ).execute()

    return file["id"]

def write(path_or_id: str, data: bytes) -> str:
    """
    Overwrites an existing Drive file.
    Accepts:
      - drive:/path/to/file
      - raw file ID
    Returns the file ID.
    """
    if is_drive_path(path_or_id):
        file_id = resolve_drive_path(path_or_id)
        if not file_id:
            raise FileNotFoundError(path_or_id)
    else:
        file_id = path_or_id
    if (type(data) == str):
        data = data.encode("utf-8")
    media = MediaIoBaseUpload(
        io.BytesIO(data),
        mimetype="application/octet-stream"
    )

    file = drive.files().update(
        fileId=file_id,
        media_body=media,
        fields="id"
    ).execute()

    return file["id"]

def delete(drive_path_or_id: str) -> None:
    """
    Deletes a file or folder from Drive.
    Accepts:
      - drive:/path/to/item
      - raw file/folder ID
    """
    # Resolve virtual path → ID
    if is_drive_path(drive_path_or_id):
        file_id = resolve_drive_path(drive_path_or_id)
        if not file_id:
            raise FileNotFoundError(drive_path_or_id)
    else:
        file_id = drive_path_or_id  # already an ID

    # Perform the delete
    drive.files().delete(fileId=file_id).execute()

def add_file(path: str) -> str:
    """
    Uploads ref/empty to a Drive path.
    Returns the file ID.
    """
    local = SCRIPT_DIR / "ref" / "empty"
    return create(local, path)

def get_folders(root: str) -> list[str]:
    root = Path(root)
    results = []

    for dirpath, dirnames, filenames in os.walk(root):
        rel = str(Path(dirpath).relative_to(root))
        if (not (len(rel) == 0 or len(rel) == 1)):
            results.append(str(rel).replace("\\", "/"))  # Normalize to forward slashes

    return results
def get_files(root: str) -> list[str]:
    root = Path(root)
    results = []

    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            full = Path(dirpath) / filename
            rel = full.relative_to(root)
            results.append(rel.as_posix())  # normalize to forward slashes

    return results
def parent_json_path(path: str) -> str:
    """
    Converts any path of the form:
        drive:/<prefix...>/<parent>/<folder>/<folder>.json
    Into:
        drive:/<prefix...>/<parent>/<parent>.json
    """
    if not is_drive_path(path):
        raise ValueError("Expected a drive:/ path")

    parts = drive_path_to_components(path)

    # Need at least: <parent>/<folder>/<file>
    if len(parts) < 3:
        raise ValueError("Path too short to transform")

    parent = parts[-3]

    # prefix = everything before <parent>
    prefix = parts[:-3]

    new_parts = prefix + [parent, parent + ".json"]

    return "drive:/" + "/".join(new_parts)
def local_json_path(path: str) -> str:
    """
    Given a path of the form:
        drive:/<prefix...>/<parent>/<folder>/<folder>.json

    Return the local JSON path:
        drive:/<prefix...>/<parent>/<folder>/<folder>.json
    (i.e., the JSON for the folder itself)
    """
    if not is_drive_path(path):
        raise ValueError("Expected a drive:/ path")

    parts = drive_path_to_components(path)

    # Need at least: <parent>/<folder>/<file>
    if len(parts) < 3:
        raise ValueError("Path too short to transform")

    folder = parts[-2]      # the folder name
    prefix = parts[:-2]     # everything before <folder>

    new_parts = prefix + [folder, folder + ".json"]

    return "drive:/" + "/".join(new_parts)

def dict_to_json(d: dict) -> str:
    return json.dumps(d, ensure_ascii=False, indent=2)

def join_json(a: str, b: str) -> str:
    dict_a = json.loads(a) if a.strip() else {}
    dict_b = json.loads(b) if b.strip() else {}
    merged = {**dict_a, **dict_b}
    return json.dumps(merged, ensure_ascii=False, indent=2)

def create_structure(local_path):
    folders = get_folders(local_path)
    name = Path(local_path).name
    local_drive_path = "drive:/3DS Storefront/" + name
    storefront_id = add_file(local_drive_path + "/"+name+".json")
    for folder in folders:
        print("Processing folder:", folder)
        name = Path(folder).name
        drive_folder_path = local_drive_path + "/" + folder
        file_path = drive_folder_path + "/"+name+".json"
        parent_json = parent_json_path(file_path)
        parent_id = resolve_drive_path(parent_json)
        json_dict = {
            "<parent directory>":[
                parent_id,
                "storefront"
            ]
        }
        with open("temp", "w") as f:
            f.write(dict_to_json(json_dict))
        create("temp", file_path)
        os.remove("temp")
        json_dict = {
            name:[
                resolve_drive_path(file_path),
                "storefront"
            ]
        }
        json_string = dict_to_json(json_dict)
        parent_json_data = read(parent_json)
        json_string = join_json(parent_json_data, json_string)
        write(parent_json, json_string.encode("utf-8"))
        print(f"Created folder: {drive_folder_path}")
    return storefront_id

def list_remote_tree(root_drive_path):
    """
    Returns:
      remote_folders: set of canonical folder paths
      remote_files: set of canonical file paths
    """
    name = Path(root_drive_path).name
    root_json = f"{root_drive_path}/{name}.json"

    remote_folders = set()
    remote_files = set()

    # BFS through JSON manifests
    queue = [(root_drive_path, root_json)]

    while queue:
        folder_drive_path, json_path = queue.pop(0)

        data = read(json_path)
        d = json.loads(data)

        for key, value in d.items():
            if key == "<parent directory>":
                continue

            # Folder entry
            if value[1] == "storefront":
                child_folder = f"{folder_drive_path}/{key}"
                remote_folders.add(child_folder)

                child_json = f"{child_folder}/{key}.json"
                queue.append((child_folder, child_json))

            # File entry
            else:
                filename = value[1]  # ALWAYS use the filename with extension
                remote_files.add(f"{folder_drive_path}/{filename}")

    return remote_folders, remote_files
def push(local_path, add_only=False):
    name = Path(local_path).name
    root_drive_path = f"drive:/3DS Storefront/{name}"
    root_json = f"{root_drive_path}/{name}.json"

    # Ensure root JSON exists
    storefront_id = resolve_drive_path(root_drive_path)
    if resolve_drive_path(root_json) is None:
        add_file(root_json)

    # ------------------------------------------------------------
    # 1. Incremental folder creation (fixed)
    # ------------------------------------------------------------
    for folder in get_folders(local_path):
        print("Processing folder:", folder)
        folder_name = Path(folder).name

        # Local parent (e.g. "." for top-level, "folder1" for "folder1/test_folder1")
        local_parent = Path(folder).parent.as_posix()
        if local_parent == ".":
            parent_folder_drive = root_drive_path
        else:
            parent_folder_drive = f"{root_drive_path}/{local_parent}"

        drive_folder_path = f"{root_drive_path}/{folder}"
        folder_json_path = f"{drive_folder_path}/{folder_name}.json"

        # Get the *folder* that should contain this folder
        parent_folder_id = resolve_drive_path(parent_folder_drive)

        # Correct existence check: is there already a child named folder_name under parent_folder_id?
        existing = find_child(parent_folder_id, folder_name)
        if existing:
            # Folder already exists on Drive → skip
            continue

        # Folder missing → create its JSON and parent JSON entry
        parent_json = parent_json_path(folder_json_path)
        parent_json_id = resolve_drive_path(parent_json)

        json_dict = {
            "<parent directory>": [
                parent_json_id,
                "storefront"
            ]
        }

        with open("temp", "w") as f:
            f.write(dict_to_json(json_dict))
        create("temp", folder_json_path)
        os.remove("temp")

        # Add folder entry to parent JSON
        entry = {
            folder_name: [
                resolve_drive_path(folder_json_path),
                "storefront"
            ]
        }

        parent_json_data = read(parent_json)
        parent_dict = json.loads(parent_json_data)

        if folder_name not in parent_dict:
            parent_dict.update(entry)
            write(parent_json, json.dumps(parent_dict, ensure_ascii=False, indent=2).encode("utf-8"))


    # ------------------------------------------------------------
    # 2. Incremental file upload
    # ------------------------------------------------------------
    for file in get_files(local_path):
        print("Processing file:", file)
        drive_file_path = f"{root_drive_path}/{file}"
        local_file_path = Path(local_path) / file

        folder_json = local_json_path(drive_file_path)
        folder_id = resolve_drive_path(Path(folder_json).parent.as_posix())

        file_name = Path(file).name

        # Check if file already exists
        existing = find_child(folder_id, file_name)
        if existing:
            # File exists → skip upload
            continue

        # Upload file
        file_id = create(str(local_file_path), drive_file_path)

        # Update folder JSON
        folder_json_data = read(folder_json)
        folder_dict = json.loads(folder_json_data)

        if file_name not in folder_dict:
            folder_dict[file_name] = [
                DIRECT + file_id,
                file_name,
                FILE_LOCATIONS.get(Path(file).suffix.lower(), "/3DS Storefront/Other")
            ]
            write(folder_json, json.dumps(folder_dict, ensure_ascii=False, indent=2).encode("utf-8"))

    # ------------------------------------------------------------
    # 3. Delete remote items not present locally
    # ------------------------------------------------------------
    if not add_only:
        # Build local sets (full drive paths including filenames)
        local_folders = {f"{root_drive_path}/{f}" for f in get_folders(local_path)}
        local_files = {f"{root_drive_path}/{f}" for f in get_files(local_path)}

        # Build remote sets (from JSON only)
        remote_folders, remote_files = list_remote_tree(root_drive_path)

        # --- DELETE FOLDERS ---
        folders_to_delete = sorted(remote_folders - local_folders, key=lambda p: p.count("/"), reverse=True)

        for folder in folders_to_delete:
            print("Processing folder:", folder)
            folder_name = Path(folder).name
            folder_json = f"{folder}/{folder_name}.json"
            parent_json = parent_json_path(folder_json)

            # Remove from parent JSON
            parent_dict = json.loads(read(parent_json))
            if folder_name in parent_dict:
                del parent_dict[folder_name]
                write(parent_json, dict_to_json(parent_dict).encode("utf-8"))

            delete(folder_json)
            delete(folder)

        # --- DELETE FILES ---
        files_to_delete = remote_files - local_files

        for file in files_to_delete:
            print("Processing file:", file)
            file_name = Path(file).name
            folder_json = local_json_path(file)

            parent_dict = json.loads(read(folder_json))

            if file_name in parent_dict:
                del parent_dict[file_name]
                write(folder_json, dict_to_json(parent_dict).encode("utf-8"))

            delete(file)
    return storefront_id

def add_files(local_path):
    files = get_files(local_path)
    name = Path(local_path).name
    local_drive_path = "drive:/3DS Storefront/" + name
    for file in files:
        print("Processing file:", file)
        drive_file_path = local_drive_path + "/" + file
        local_file_path = Path(local_path) / file
        create(str(local_file_path), drive_file_path)
        json_path = local_json_path(drive_file_path)
        file_name = Path(file).name
        json_dict = {
            file_name:[
                DIRECT+resolve_drive_path(drive_file_path),
                file_name,
                FILE_LOCATIONS.get(Path(file).suffix.lower(), "/3DS Storefront/Other")
            ]
        }
        curr_json_data = read(json_path)
        json_string = join_json(curr_json_data, dict_to_json(json_dict))
        write(json_path, json_string.encode("utf-8"))
        print(f"Added file: {drive_file_path}")

if __name__ == "__main__":
    usage = "Usage: \nstorefront create <local_storefront_path>\nstorefront push <local_storefront_path> (--add-only)\nstorefront delete <storefront_name>\nstorefront login\nstorefront logout"
    if (len(sys.argv) == 1):
        print(usage)
        sys.exit(0)
    elif (sys.argv[1] == "logout" or sys.argv[1] == "login"):
        pass
    elif (len(sys.argv) < 3 or len(sys.argv) > 4):
        print(usage)
        sys.exit(0)
    elif (len(sys.argv) == 4):
        if (sys.argv[3] != "--add-only"):
            print(usage)
            sys.exit(0)
    command = sys.argv[1]
    if (command != "logout"):
        drive = auth()
        if Path.exists(SCRIPT_DIR / "token.json"):
            with open(SCRIPT_DIR / "token.json", "rb") as src, open(SCRIPT_DIR / "private_token.json", "wb") as dst:
                dst.write(src.read())
            os.remove(SCRIPT_DIR / "token.json")
        if (command == "login"):
            print(usage)
            sys.exit(0)
    for i in range(10):
        try:
            if (command == "create"):
                local_path = sys.argv[2]
                storefront_id = create_structure(local_path)
                command = "push"  #  This way, if it fails, it picks back up where it left off.
                add_files(local_path)
                print(f"Storefront created with ID: {storefront_id}\n Feel free to DM the ID to the dev (https://discord.com/users/756198884233183262) for it to be added to the hub.")
                sys.exit(0)
            elif (command == "push"):
                local_path = sys.argv[2]
                add_only = (len(sys.argv) == 4 and sys.argv[3] == "--add-only")
                storefront_id = push(local_path, add_only)
                print(f"Storefront updated. ID reminder: {storefront_id}\n Feel free to DM the ID to the dev (https://discord.com/users/756198884233183262) for it to be added to the hub.")
                sys.exit(0)
            elif (command == "delete"):
                local_path = sys.argv[2]
                add_only = (len(sys.argv) == 4 and sys.argv[3] == "--add-only")
                delete(local_path, add_only)
                sys.exit(0)
            elif (command == "logout"):
                if (Path.exists(SCRIPT_DIR / "private_token.json")):
                    os.remove(SCRIPT_DIR / "private_token.json")
                print("Logged out.")
                sys.exit(0)
        except HttpError as e:
            pass