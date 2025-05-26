import PySimpleGUI as sg
import pandas as pd
import splitter
import grouper
import ui
import utils
import os

def read_csv_with_encoding(file_path, sku_col, id_col, encoding):
    """Helper function to read CSV with specified encoding."""
    try:
        window['status'].update(f'Reading with {encoding} encoding...')
        window.refresh()
        
        # Read the file with the specified encoding
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            dtype=str,
            on_bad_lines='warn',
            engine='python'
        )
        
        # Clean up the data
        skus = df[sku_col].str.strip().fillna('').tolist()
        ids = df[id_col].str.strip().fillna('').tolist()
        
        return skus, ids
        
    except Exception as e:
        raise Exception(f"Failed to read with {encoding}: {str(e)}")

def main():
    # Create main window
    window = ui.create_window()
    
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
            
        # Handle Load Preview button
        if event == 'Load Preview':
            if values['csv_file'] and os.path.exists(values['csv_file']):
                try:
                    # Determine selected encoding
                    encoding = 'shift_jis' if values['shift_jis'] else 'utf-8'
                    
                    # Try to read the file with the selected encoding
                    try:
                        df = pd.read_csv(values['csv_file'], 
                                    encoding=encoding,
                                    nrows=10,  # Only read first 10 rows for preview
                                    dtype=str)
                        
                        # Show preview in the preview window
                        preview_text = df.head().to_string()
                        window['preview'].update(preview_text)
                        window['status'].update('Preview loaded successfully')
                        
                        # Auto-detect columns if they exist
                        possible_sku_cols = [col for col in df.columns if 'sku' in col.lower()]
                        possible_id_cols = [col for col in df.columns if 'id' in col.lower() or 'catalog' in col.lower()]
                        
                        if possible_sku_cols:
                            window['sku_col'].update(possible_sku_cols[0])
                        if possible_id_cols:
                            window['id_col'].update(possible_id_cols[0])
                            
                    except Exception as e:
                        window['status'].update(f'Error loading preview: {str(e)}')
                        
                except Exception as e:
                    sg.popup_error(f"Error loading preview: {str(e)}")
            else:
                sg.popup_error("Please select a valid CSV file first")
                    
        # Handle Process & Export
        if event == 'Process & Export':
            try:
                # Validate inputs
                if not values['csv_file'] or not os.path.exists(values['csv_file']):
                    raise ValueError("Please select a valid CSV file")
                    
                if not values['sku_col'] or not values['id_col']:
                    raise ValueError("Please specify both SKU and ID column names")
                
                # Get selected encoding
                encoding = 'shift_jis' if values['shift_jis'] else 'utf-8'
                
                # Update UI
                window['status'].update(f'Starting export with {encoding} encoding...')
                window.refresh()
                
                # 1. Read the CSV file
                window['status'].update('Reading CSV file...')
                window.refresh()
                
                try:
                    skus, ids = read_csv_with_encoding(
                        values['csv_file'], 
                        values['sku_col'], 
                        values['id_col'],
                        encoding
                    )
                    print(f"Successfully read {len(skus)} rows")
                except Exception as e:
                    raise Exception(f"Error reading CSV file: {str(e)}")
                
                # 2. Process the data
                window['status'].update('Processing data...')
                window.refresh()
                
                try:
                    result = grouper.group_by_catalog(skus, ids)
                    print(f"Processed {len(result)} results")
                except Exception as e:
                    raise Exception(f"Error processing data: {str(e)}")
                
                # 3. Prepare output directory
                output_dir = 'output'
                os.makedirs(output_dir, exist_ok=True)
                
                # 4. Generate output filename with timestamp
                timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
                base_name = os.path.splitext(os.path.basename(values['csv_file']))[0]
                output_filename = f"{base_name}_processed_{timestamp}.csv"
                output_path = os.path.join(output_dir, output_filename)
                
                # 5. Write the output file
                window['status'].update('Writing output file...')
                window.refresh()
                
                try:
                    utils.write_csv(output_path, result)
                    print(f"Successfully wrote output to {output_path}")
                except Exception as e:
                    raise Exception(f"Error writing output file: {str(e)}")
                
                # 6. Show success message
                success_msg = (
                    f"✅ Processing complete!\n\n"
                    f"• Input file: {os.path.basename(values['csv_file'])}\n"
                    f"• Output file: {output_filename}\n"
                    f"• Location: {os.path.abspath(output_path)}"
                )
                
                window['status'].update('Export completed successfully')
                sg.popup_ok(success_msg, title='Export Complete')
                
                # 7. Open the output directory in file explorer
                try:
                    if os.name == 'nt':  # Windows
                        os.startfile(os.path.abspath(output_dir))
                    elif os.name == 'posix':  # macOS, Linux
                        os.system(f'open "{os.path.abspath(output_dir)}"')
                except:
                    pass  # Don't fail if we can't open the folder
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error during export: {error_msg}")
                window['status'].update('Export failed')
                sg.popup_error(
                    f"❌ Export Failed\n\n{error_msg}",
                    title='Export Error',
                    keep_on_top=True
                )
                
    window.close()

if __name__ == '__main__':
    main()
