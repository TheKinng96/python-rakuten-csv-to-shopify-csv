import PySimpleGUI as sg
import splitter
import grouper
import ui
import utils

def main():
    # Create main window
    window = ui.create_window()
    
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
            
        # Handle Load button
        if event == 'Load':
            if values['csv_file']:
                try:
                    ui.update_preview(window, values['csv_file'])
                except Exception as e:
                    sg.popup_error(f"Error loading CSV: {str(e)}")
                    
        # Handle Process & Export
        if event == 'Process':
            if values['csv_file'] and values['sku_col'] and values['id_col']:
                try:
                    # Read CSV
                    skus, ids = utils.read_csv(values['csv_file'], values['sku_col'], values['id_col'])
                    
                    # Process data
                    result = grouper.group_by_catalog(skus, ids)
                    
                    # Write output
                    utils.write_csv('output.csv', result)
                    sg.popup('Processing complete! Output saved as output.csv')
                except Exception as e:
                    sg.popup_error(f"Error processing CSV: {str(e)}")
    
    window.close()

if __name__ == '__main__':
    main()
