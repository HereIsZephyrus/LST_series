# LST_series

calculation land surface temperature using Landsat and MODIS series

## Usage

### Environment setup

install python environment using conda

```bash
conda create -f gee_py_env.yml
conda activate gee
```

create a `.env` file in the root directory and add the following variables:

```bash # .env
IMAGE_SAVE_PATH=your local path to save images (required)
RECORD_FILE_PATH=your local path to save record file (required)
SERIES_FOLDER_ID=google drive folder id (required)
```

### Google authentication

1. run gee_launch.ipynb
2. download google oauth2.0 file and name it `client_secret.json`, put it in the root directory
3. choose your own gee project

### Export LST image to Google Drive and Download

```bash
python workflow_image.py
```