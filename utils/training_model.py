from logs.application_logging import logger
from utils import data_preprocessing, load_data, clustering, tuner, file_method
from sklearn.model_selection import train_test_split

class train_model:
    def __init__(self):
        self.logger = logger.AppLogger()
        self.file_object = open("logs/training_logs/training_model_log.txt", 'a+')

    def model_training(self):
        self.logger.log(self.file_object, "Training Started")
        try:
            fetch_data = load_data.get_data(self.file_object, self.logger)
            data = fetch_data.extract_data()

            data_preprocessor = data_preprocessing.data_preprocessing(self.file_object, self.logger)
            data = data_preprocessor.remove_columns(data, ["Wafer"])
            feature, label = data_preprocessor.seperate_label_feature(data, "Ouput")
            is_null_present = data_preprocessor.is_null_present(feature)
            if is_null_present:
                feature = data_preprocessor.handle_missing_values(feature)
            columns_to_drop = data_preprocessor.get_columns_with_zero_std_deviation(feature)
            feature = data_preprocessor.remove_columns(feature, columns_to_drop)

            kmeans = clustering.kmeans_clustering(self.file_object, self.logger)
            no_of_clusters = kmeans.elbow_plot(feature)
            feature = kmeans.create_cluster(feature, no_of_clusters)
            feature['Labels'] = label
            list_of_clusters = feature['Labels'].unique()

            for cluster in list_of_clusters:
                cluster_data = feature[feature['Labels'] == cluster]
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis = 1)
                cluster_label = cluster_data['Labels']

                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size = 1/3, random_state = 355)
                model_finder = tuner.model_finder(self.file_object, self.logger)
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                file_op = file_method.file_operation(self.file_object, self.logger)
                save_model = file_op.save_model(best_model, best_model_name + str(cluster))

            self.logger.log(self.file_object, "Training Successfull")
            self.file_object.close()

        except Exception as e:
            self.logger.log(self.file_object, "Error occured while training")
            self.file_object.close()
            raise Exception()