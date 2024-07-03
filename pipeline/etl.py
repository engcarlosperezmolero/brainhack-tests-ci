import pandas as pd
#changing something

def ingest_csv(file_path):
    """Ingesta un archivo CSV y lo convierte en un DataFrame de pandas. a"""
    return pd.read_csv(file_path)


def normalize_column(df, column_name):
    """Normaliza una columna del DataFrame."""
    df[column_name] = (df[column_name] - df[column_name].mean()) / df[column_name].std()
    return df

def clean_and_transform(df):
    """Limpia y transforma el DataFrame."""
    # Elimina filas con valores nulos
    df = df.dropna()

    # Transformación: convertir la columna 'value' a entero
    if 'value' in df.columns:
        df['value'] = df['value'].astype(int)
        df = normalize_column(df, 'value')

    if 'name' in df.columns:
        df['name'] = df['name'].str.lower()

    # Agrega una nueva columna 'incoming' con valores booleanos
    df['incoming'] = True

    return df


def save_csv(df, file_path):
    """Guarda el DataFrame transformado a un nuevo archivo CSV."""
    df.to_csv(file_path, index=False)

# Proceso ETL completo
def etl_process(input_file, output_file):
    df = ingest_csv(input_file)
    cleaned_df = clean_and_transform(df)
    save_csv(cleaned_df, output_file)
