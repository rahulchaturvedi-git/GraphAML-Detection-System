from app.extractor import FeatureExtractor
from app.feature_exporter import FeatureExporter


def main():

    extractor = FeatureExtractor()

    exporter = FeatureExporter()

    features = extractor.extract_transactions()

    exporter.export(features)


if __name__ == "__main__":
    main()