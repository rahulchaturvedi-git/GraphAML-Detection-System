import joblib

def load_model():
    model = joblib.load("ml/aml_model.pkl")
    return model