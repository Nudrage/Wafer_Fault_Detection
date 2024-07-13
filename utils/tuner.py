import xgboost
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.ensemble import RandomForestClassifier

class model_finder:
    def __init__(self, file_object, logger_object):
        self.logger = logger_object
        self.file_object = file_object

    def get_best_model(self, x_train, y_train, x_test, y_test):
        self.logger.log(self.file_object, 'Entered the get_best_model method of the tuner class')
        try:
            self.xgboost = self.get_param_xgboost(x_train, y_train)
            self.predictions = self.xgboost.predict(x_test)

            if len(y_test.unique()) == 1: #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.xgboost_score = accuracy_score(y_test, self.predictions)
                self.logger.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(y_test, self.predictions)
                self.logger.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score))

            self.random_forest = self.get_param_random_forest(x_train, y_train)
            self.predictions = self.random_forest.predict(x_test)

            if len(y_test.unique()) == 1: #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.random_forest_score = accuracy_score(y_test, self.predictions)
                self.logger.log(self.file_object, 'Accuracy for RandomForest:' + str(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(y_test, self.predictions)
                self.logger.log(self.file_object, 'AUC for RandomForest:' + str(self.random_forest_score))

            if (self.xgboost_score < self.random_forest_score):
                return 'XGBoost', self.xgboost
            else:
                return 'RandomForest', self.random_forest

        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in get_best_model method of the tuner class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Model Selection Failed. Exited the get_best_model method of the tuner class')
            raise Exception()

    def get_param_xgboost(self, x_train, y_train):
        self.logger.log(self.file_object, 'Entered the get_param_xgboost method of the tuner class')
        try:
            self.param_grid_xgboost = {
                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth': [3, 5, 10, 20],
                'n_estimators': [10, 50, 100, 200]
            }
            self.grid_xgboost = GridSearchCV(xgboost.XGBClassifier(objective='binary:logistic'), self.param_grid_xgboost, verbose=3)
            self.grid_xgboost.fit(x_train, y_train)

            self.learning_rate = self.grid_xgboost.best_params_['learning_rate']
            self.max_depth = self.grid_xgboost.best_params_['max_depth']
            self.n_estimators = self.grid_xgboost.best_params_['n_estimators']

            self.xgb_classifier = xgboost.XGBClassifier(learning_rate = self.learning_rate, max_depth = self.max_depth, n_estimators = self.n_estimators)
            self.xgb_classifier.fit(x_train, y_train)

            self.logger.log(self.file_object, 'XGBoost best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb_classifier

        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()
        
    def get_param_random_forest(self, x_train, y_train):
        self.logger.log(self.file_object, 'Entered the get_param_random_forest method of the tuner class')
        try:
            self.param_grid_random_forest = {
                'n_estimators': [10, 50, 100, 130], 
                'criterion': ['gini', 'entropy'],
                'max_depth': [2, 3, 5, 10, 50],
                'max_features': ['auto', 'log2']
            }
            self.grid_random_forest = GridSearchCV(estimator = self.random_forest, param_grid = self.param_grid_random_forest, cv = 5,  verbose = 3)
            self.grid_random_forest.fit(x_train, y_train)

            self.n_estimators = self.grid_random_forest.best_params_['n_estimators']
            self.criterion = self.grid_random_forest.best_params_['criterion']
            self.max_depth = self.grid_random_forest.best_params_['max_depth']
            self.max_features = self.grid_random_forest.best_params_['max_features']

            self.random_forest = RandomForestClassifier(n_estimators = self.n_estimators, criterion = self.criterion, max_depth = self.max_depth, max_features = self.max_features)
            self.random_forest.fit(x_train, y_train)
            self.logger.log(self.file_object, 'Random Forest best params: ' + str(self.grid_random_forest.best_params_) + '. Exited the get_best_params_for_random_forest method of the Model_Finder class')

            return self.random_forest
    
        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in get_best_params_for_random_forest method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Random Forest Parameter tuning  failed. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise Exception()
