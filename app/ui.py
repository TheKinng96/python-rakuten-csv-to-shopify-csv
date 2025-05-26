import PySimpleGUI as sg

def create_window():
    """Create the main application window."""
    layout = [
        [sg.Text('CSV File:'), sg.Input(key='csv_file'), sg.FileBrowse()],
        [sg.Text('SKU Column:'), sg.Input(key='sku_col')],
        [sg.Text('ID Column:'), sg.Input(key='id_col')],
        [sg.Text('File Encoding:')],
        [sg.Radio('Shift-JIS (Recommended for Japanese CSVs)', 'ENCODING', default=True, key='shift_jis'),
         sg.Radio('UTF-8', 'ENCODING', key='utf8')],
        [sg.Button('Load Preview'), sg.Button('Process & Export')],
        [sg.ProgressBar(100, orientation='h', size=(20, 20), key='progress')],
        [sg.Text('Status: Ready', key='status')],
        [sg.Multiline(size=(60, 10), key='preview', disabled=True)]
    ]
    
    return sg.Window('CSV Processing Tool', layout, finalize=True)

def update_preview(window, csv_file, encoding='shift_jis'):
    """
    Update the preview with the first few rows of the CSV.
    
    Args:
        window: The PySimpleGUI window object
        csv_file (str): Path to the CSV file
        encoding (str): Encoding to use (default: 'shift_jis')
    """
    try:
        # Try to read the first 10 lines with the specified encoding
        with open(csv_file, 'r', encoding=encoding, errors='replace') as f:
            lines = []
            for _ in range(10):  # Read up to 10 lines
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            
            preview_text = ''.join(lines)
            window['preview'].update(preview_text)
            window['status'].update(f'Preview loaded with {encoding} encoding')
            
    except Exception as e:
        error_msg = f'Error loading CSV with {encoding}: {str(e)}'
        window['status'].update(error_msg)
        window['preview'].update(error_msg)
        raise Exception(error_msg)
