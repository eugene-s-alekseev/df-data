def iris_py37_train():
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report

    from src.model_drivers import ModelWriter
    from src.readers import Reader

    processor_backend_name = "py37"

    reader = Reader(app_name="iris_train",
                    processor_backend_name=processor_backend_name)

    X_train = reader.read_by_draft_name("iris_features")
    y_train = reader.read_by_draft_name("iris_target")

    model = LogisticRegression()
    model.fit(X_train, y_train)

    predicted = model.predict(X_train)

    report = classification_report(y_train, predicted, output_dict=True)
    metrics = report["macro avg"]

    model_writer = ModelWriter(processor_backend_name)
    # здесь нужно явно указать, что на каком model backend'е натренирована эта модель
    model_writer.write_by_model_name(model,
                                     metrics,
                                     "sklearn_backend",
                                     "iris_abstract_model",
                                     "logistic_regression_baseline")


def main():
    iris_py37_train()


if __name__ == "__main__":
    main()
