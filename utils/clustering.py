from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from kneed import KneeLocator
from utils import file_method

class kmeans_clustering():
    def __init__(self, file_object, logger_object):
        self.logger = logger_object
        self.file_object = file_object

    def elbow_plot(self, data):
        self.logger.log(self.file_object, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss = []
        try:
            for no_of_cluster in range(1, 11):
                kmeans = KMeans(n_clusters = no_of_cluster, init = 'k-means++', random_state = 42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            plt.savefig('data/preprocessing_data/K-Means_Elbow.PNG')

            self.kn = KneeLocator(range(1, 11), wcss, curve = 'convex', direction = 'decreasing')
            self.logger.log(self.file_object, 'Finding the optimum number of clusters is a success. Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Finding the optimum number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()
        
    def create_cluster(self, data, no_of_clusters):
        self.logger.log(self.file_object, 'Entered the create_cluster method of the KMeansClustering class')
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters = no_of_clusters, init = 'k-means++', random_state = 42)
            self.y_kmeans = self.kmeans.fit_predict(data)

            self.file_operation = file_method.file_operations(self.file_object, self.logger)
            self.save_model = self.file_operation.save_model(self.kmeans, 'KMeans')
            self.data['Cluster'] = self.y_kmeans
            self.logger.log(self.file_object, 'succesfully created ' +str(self.kn.knee) + 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in create_cluster method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Fitting the data to clusters failed. Exited the create_cluster method of the KMeansClustering class')
            raise Exception()
