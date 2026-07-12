from pathlib import Path
from datetime import datetime

import pandas as pd


class FeatureExporter:

    def export(self, features):

        df = pd.DataFrame(features)

        output_dir = Path("../../data/features")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = output_dir / f"graph_features_{timestamp}.parquet"

        df.to_parquet(file_path, index=False)

        print(f"\nExported {len(df)} feature vectors")
        print(f"Saved to: {file_path}")

        return file_path