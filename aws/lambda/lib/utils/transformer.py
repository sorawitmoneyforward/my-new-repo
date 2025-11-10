from sklearn.base import BaseEstimator, TransformerMixin


class UseUnderBt(BaseEstimator, TransformerMixin):
    def __init__(self, use_under_Bt_remove_over_Bt_plus_one=0):
        self.use_under_Bt_remove_over_Bt_plus_one = use_under_Bt_remove_over_Bt_plus_one

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # 直接の書き換えが起きないようにcopy
        _X = X.copy()

        tmp_list = ["B" + str(i + 1) + "_monthly_" for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)]
        tmp_list.extend(["cum_" + str(i + 2) for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)])
        tmp_list.extend(["sd_" + str(i + 2) for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)])
        # print(tmp_list)

        for i in tmp_list:
            _X = _X.drop(_X.filter(like=i, axis=1).columns, axis=1)  # Btに応じて、カラムを削除

        __X = _X.dropna()  # NAが入っているレコードを削除

        return __X
