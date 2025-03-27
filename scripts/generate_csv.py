import pandas as pd
import numpy as np

# Buat DataFrame dengan 3 juta baris
rows = 3_000_000
data = {
    "id": np.arange(1, rows + 1),
    "value": np.random.randint(1, 100, size=rows)
}

df = pd.DataFrame(data)

# Simpan ke CSV
df.to_csv("../data/input.csv", index=False)
print("CSV file created successfully!")
