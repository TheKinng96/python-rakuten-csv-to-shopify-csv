import PySimpleGUI as sg

def create_window():
    """Create the main application window."""
    layout = [
        [sg.Text('CSV File:'), sg.Input(key='csv_file'), sg.FileBrowse()],
        [sg.Text('SKU Column:'), sg.Input(key='sku_col')],
        [sg.Text('ID Column:'), sg.Input(key='id_col')],
        [sg.Button('Load'), sg.Button('Process & Export')],
        [sg.ProgressBar(100, orientation='h', size=(20, 20), key='progress')],
        [sg.Text('', key='status')],
        [sg.Multiline(size=(60, 10), key='preview', disabled=True)]
    ]
    
    return sg.Window('CSV Processing Tool', layout)

def update_preview(window, csv_file):
    """Update the preview with the first few rows of the CSV."""
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()[:10]  # Show first 10 lines
            preview_text = ''.join(lines)
            window['preview'].update(preview_text)
            window['status'].update('CSV loaded successfully')
    except Exception as e:
        window['status'].update(f'Error loading CSV: {str(e)}')
