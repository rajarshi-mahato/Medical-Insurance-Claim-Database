import pandas as pd
import numpy as np
import os

print("Generating 1 Million rows of medical claims... (This might take a few seconds)")

# Define the number of rows
n = 1000000

# Generate dummy data using NumPy for speed
df = pd.DataFrame({
    'claim_id': np.arange(1, n + 1),
    'patient_id': np.random.randint(1000, 9999, size=n),
    'diagnosis': np.random.choice(['Flu', 'Surgery', 'X-Ray', 'Checkup', 'Bloodwork', 'MRI', 'Therapy'], size=n),
    'claim_amount': np.random.uniform(50.0, 10000.0, size=n).round(2),
    'status': np.random.choice(['Approved', 'Denied', 'Pending'], size=n)
})

output_path = os.path.expanduser('~/med_claims_d/landing/large_claims_1M.csv')
df.to_csv(output_path, index=False)

print(f"Done! 1 Million rows saved to {output_path}")
