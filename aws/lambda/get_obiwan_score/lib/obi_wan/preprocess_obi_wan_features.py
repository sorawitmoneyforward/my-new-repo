import pandas as pd


def preprocess_obi_wan_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Obi-Wan特徴量データの前処理
    @see https://colab.research.google.com/drive/1o_M6P_Eqke58LSMfsO_vvgZwZaZZCZ3e#scrollTo=FJLnuw2HMTJG
    """
    # boolean型のカラムをintに変換
    df_boolean = df.select_dtypes(include="boolean")
    df_boolean = df_boolean.astype("int")

    # 前カラム名一覧と元boolean型のカラム名一覧を作る
    all_columns_names = list(df.columns)
    boolean_columns_names = list(df_boolean.columns)

    # dataframeからbooleanのカラムを除外
    df_revise = df.drop(boolean_columns_names, axis=1)
    # booleanを0,1に変えたカラムを結合
    df_revise = pd.concat([df_revise, df_boolean], axis=1)
    # カラムの順番を元に戻す
    return df_revise.reindex(columns=all_columns_names)
