import pytest
import pandas as pd
import pandas.testing as pdt
import numpy as np
from pipeline.etl import ingest_csv, clean_and_transform, save_csv, normalize_column

# Datos de prueba
data = {
    'name': ['Alicia', 'Nacho', 'Charly', None],
    'value': ['1', '2', '3', '4']
}
input_df = pd.DataFrame(data)

# DataFrame esperado después de la limpieza y transformación
expected_data = {
    'name': ['alicia', 'nacho', 'charly'],
    'value': [-1, 0, 1],
    'incoming': [True, True, True]
}
expected_df = pd.DataFrame(expected_data)



def test_clean_and_transform():
    # Probar la función de limpieza y transformación
    result_df = clean_and_transform(input_df)
    pdt.assert_frame_equal(result_df, expected_df, check_dtype=False, check_like=True)


def test_normalize_column():
    df = pd.DataFrame({'value': [1, 2, 3, 4, 5]})
    normalized_df = normalize_column(df, 'value')
    assert np.isclose(normalized_df['value'].mean(), 0)
    assert np.isclose(normalized_df['value'].std(), 1)
