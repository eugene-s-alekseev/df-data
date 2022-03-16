def iris_py37_prediction():
    from src.model_drivers import ModelReader

    processor_backend_name = "py37"
    model_reader = ModelReader(processor_backend_name)

    model = model_reader.read_by_model_name("iris_abstract_model")
    model.predict_by_app_name("iris_predict")


def main():
    iris_py37_prediction()


if __name__ == "__main__":
    main()
