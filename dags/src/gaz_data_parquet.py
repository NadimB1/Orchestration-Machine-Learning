import pandas as pd
import os
from datetime import datetime

class CSVtoParquetProcessor:
    def __init__(self, input_folder_parquet, output_folder_parquet):
        self.input_folder_parquet = input_folder_parquet
        self.output_folder_parquet = output_folder_parquet
        self.dataframes = []
        self.current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def check_today_files_exist(self):
        today_date = datetime.now().strftime('%Y%m%d')
        for filename in os.listdir(self.output_folder_parquet):
            if today_date in filename:
                print(f"Today's files already exist: {filename}")
                return True
        return False

    def process_files(self):
        if self.check_today_files_exist():
            print("Today's files already exist. Skipping processing.")
            return
        
        for filename in os.listdir(self.input_folder_parquet):
            if filename.endswith('.csv'):
                file_path = os.path.join(self.input_folder_parquet, filename)
                df = pd.read_csv(file_path)
                
                # Normalize column names
                df.columns = df.columns.str.strip().str.lower()
                
                # Print the column names for debugging
                print(f"Columns in {filename}: {df.columns.tolist()}")
                
                # Extract the date from the filename
                date_str = filename.split('_')[2]
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Filter the DataFrame
                if 'zas' in df.columns:
                    df_filtered = df[df['zas'] == 'ZAG PARIS'].copy()
                    
                    if not df_filtered.empty:
                        # Add dates to the DataFrame
                        df_filtered.loc[:, 'file_date'] = file_date
                        df_filtered.loc[:, 'processing_date'] = self.current_timestamp
                        
                        # Append to the list
                        self.dataframes.append(df_filtered)
                    else:
                        print(f"No data for 'zag paris' in {filename}")
                else:
                    print(f"'zas' column not found in {filename}")

    def concatenate_and_save(self):
        if self.dataframes:
            combined_df = pd.concat(self.dataframes, ignore_index=True)
            
            # Define the output file path
            output_file_path = os.path.join(self.output_folder_parquet, 'ZAG_PARIS_combined_output.parquet')
            
            # Check if the Parquet file already exists
            if os.path.exists(output_file_path):
                existing_df = pd.read_parquet(output_file_path)
                combined_df = pd.concat([existing_df, combined_df]).drop_duplicates().reset_index(drop=True)
            
            # Save the combined DataFrame to the Parquet file
            combined_df.to_parquet(output_file_path, index=False)
            if os.path.exists(output_file_path):
                print(f"File updated successfully: {output_file_path}")
            else:
                print(f"Failed to update file: {output_file_path}")
        else:
            print("No data for ZAG PARIS found in the files.")

# Example usage
if __name__ == "__main__":
    input_folder_parquet = r'/opt/airflow/dags/data/gazs_output'
    output_folder_parquet = r'/opt/airflow/dags/data/gazs_output_parquet'
    
    #input_folder_parquet = r'/home/sofianne/dev/orchestration/data/gazs_output'
    #output_folder_parquet = r'/home/sofianne/dev/orchestration/data/gazs_output_parquet'
    
    processor = CSVtoParquetProcessor(input_folder_parquet, output_folder_parquet)
    processor.process_files()
    processor.concatenate_and_save()
