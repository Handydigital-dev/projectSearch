import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import paramiko
import os
from dotenv import load_dotenv
import tempfile
from datetime import datetime, timedelta, date
import json
from typing import List, Tuple

# ページ設定
st.set_page_config(layout="wide", page_title="プロジェクト検索システム")

# 環境変数の読み込み
load_dotenv()

# 環境変数の取得と検証
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
EC2_HOSTNAME = os.getenv('EC2_HOSTNAME')
EC2_USERNAME = os.getenv('EC2_USERNAME')
EC2_PRIVATE_KEY = os.getenv('EC2_PRIVATE_KEY')

# 必須の環境変数が設定されているか確認
if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, EC2_HOSTNAME, EC2_USERNAME, EC2_PRIVATE_KEY]):
    st.error("必須の環境変数が設定されていません。")
    st.stop()

# スタイルの追加
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        height: 3em;
        margin-top: 1em;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    .result-table {
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

# 商品ジャンルの対応表を読み込む
def load_product_genre_mapping():
    with open('product_genre_mapping.json', 'r', encoding='utf-8') as f:
        return json.load(f)

# 商品ジャンルの対応表
PRODUCT_GENRE_MAPPING = load_product_genre_mapping()

# SSHクライアントを作成する関数
def create_ssh_client():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # 一時的なPEMファイルを作成
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_key_file:
        temp_key_file.write(EC2_PRIVATE_KEY)
        temp_key_file_path = temp_key_file.name
    
    try:
        # ファイルのパーミッションを設定（Windowsでは効果がありませんが、互換性のために残します）
        os.chmod(temp_key_file_path, 0o600)
        
        # SSH接続
        ssh_client.connect(hostname=EC2_HOSTNAME, username=EC2_USERNAME, key_filename=temp_key_file_path)
        st.sidebar.success("EC2サーバーに接続しました。")
        return ssh_client, temp_key_file_path
    except Exception as e:
        st.sidebar.error(f"EC2サーバーへの接続に失敗しました: {str(e)}")
        return None, temp_key_file_path

# MySQLコマンドを実行する関数
def execute_mysql_command(ssh_client, mysql_command):
    try:
        create_config_command = (
            f"echo '[client]\nuser={MYSQL_USER}\npassword={MYSQL_PASSWORD}\nhost={MYSQL_HOST}' > ~/.my.cnf && chmod 600 ~/.my.cnf"
        )
        ssh_client.exec_command(create_config_command)
        stdin, stdout, stderr = ssh_client.exec_command(f"mysql --defaults-file=~/.my.cnf {MYSQL_DATABASE} -e \"{mysql_command}\"")
        
        result = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')

        ssh_client.exec_command("rm ~/.my.cnf")

        if error and "Warning" not in error:
            st.error(f"MySQLコマンド実行エラー: {error}")
            return None
        return result
    except Exception as e:
        st.error(f"MySQLコマンドの実行に失敗しました: {str(e)}")
        return None

# プロジェクトデータを取得する関数
def get_project_data(ssh_client, project_name=None, product_name=None, talent_name=None, product_genre=None, group_name=None, contact_person=None, list_price=None, created_start=None, created_end=None):
    conditions = []
    if project_name:
        conditions.append(f"p.name LIKE '%{project_name}%'")
    if product_name:
        conditions.append(f"p.product_name LIKE '%{product_name}%'")
    if talent_name:
        conditions.append(f"t.name LIKE '%{talent_name}%'")
    if product_genre:
        genre_codes = [code for code, name in PRODUCT_GENRE_MAPPING.items() if name == product_genre]
        if genre_codes:
            genre_codes_str = ','.join([f"'{code}'" for code in genre_codes])
            conditions.append(f"p.product_genre_cd IN ({genre_codes_str})")
    if group_name:
        conditions.append(f"tlg.name LIKE '%{group_name}%'")
    if contact_person:
        conditions.append(f"p.contact_person LIKE '%{contact_person}%'")
    if list_price:
        conditions.append(f"ttl.price LIKE '%{list_price}%'")
    if created_start:
        conditions.append(f"DATE(p.created) >= '{created_start}'")
    if created_end:
        conditions.append(f"DATE(p.created) <= '{created_end}'")
    
    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = f"AND {where_clause}"
    
    query = f"""
    SELECT DISTINCT
        p.id AS プロジェクトID,
        p.name AS プロジェクト名,
        p.product_name AS 商品名,
        p.product_genre_cd AS 商品ジャンルコード,
        p.contact_person AS 連絡担当者,
        p.budget AS プロジェクト予算,
        DATE(p.created) AS プロジェクト作成日,
        DATE(p.modified) AS プロジェクト更新日
    FROM 
        handy_casting.projects p
    LEFT JOIN 
        handy_casting.talent_lists tl ON p.id = tl.project_id
    LEFT JOIN 
        handy_casting.talent_list_groups tlg ON tl.id = tlg.talent_list_id
    LEFT JOIN 
        handy_casting.talents_talent_lists ttl ON tl.id = ttl.talent_list_id
    LEFT JOIN 
        handy_casting.talents t ON ttl.talent_id = t.id
    WHERE 
        p.deleted IS NULL
        AND p.viewable_team_id = 1003
        AND p.name NOT LIKE '%テスト%'
        AND p.name NOT LIKE '%てすと%'
        AND p.name NOT LIKE '%test%'
        AND p.name NOT LIKE '%試験%'
        AND p.name NOT LIKE '%〇〇%'
        {where_clause}
    ORDER BY 
        p.created DESC
    LIMIT 10000;
    """
    
    result = execute_mysql_command(ssh_client, query)
    if result:
        lines = result.strip().split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]
        df = pd.DataFrame(data, columns=headers)
        
        # 商品ジャンルコードを日本語名に変換
        df['商品ジャンル'] = df['商品ジャンルコード'].map(PRODUCT_GENRE_MAPPING)
        
        # 列の並び替え
        columns_order = ['プロジェクトID', 'プロジェクト名', '商品名', '商品ジャンル', '連絡担当者', 'プロジェクト予算', 'プロジェクト作成日', 'プロジェクト更新日']
        df = df[columns_order]
        
        # null値をハイフンに置換
        df = df.fillna('-')
        
        return df
    return None

# タレントデータを取得する関数
def get_talent_data(ssh_client, project_id, talent_list_id):
    query = f"""
        SELECT DISTINCT
            IFNULL(t.name, '-') AS タレント名,
            IFNULL(FLOOR(DATEDIFF(CURDATE(), t.birthday_for_search) / 365), '-') AS 年齢,
            IFNULL(tlg.name, '設定なし') AS グループ名,
            IFNULL(ttl.nego_level, '-') AS リスト確認状況,
            IFNULL(ttl.price, '-') AS リスト価格,
            IFNULL(ttl.memo, '-') AS リスト備考,
            IFNULL(t.hobby, '-') AS 趣味,
            IFNULL(t.skill, '-') AS 特技,
            IFNULL(t.biography, '-') AS 経歴,
            IFNULL(CONCAT(
                IFNULL(t.fee_year_cm_lower, ''),
                CASE 
                    WHEN t.fee_year_cm_lower IS NOT NULL AND t.fee_year_cm_upper IS NOT NULL THEN '～'
                    ELSE ''
                END,
                IFNULL(t.fee_year_cm_upper, '')
            ), '-') AS handy料金,
            IFNULL(t.memo, '-') AS handyメモ,
            IFNULL(ttl.sort_no, '-') AS ソート番号,
            IFNULL(DATE(ttl.created), '-') AS タレントリスト登録日
        FROM 
            handy_casting.projects p
        JOIN
            handy_casting.talent_lists tl ON p.id = tl.project_id
        JOIN
            handy_casting.talents_talent_lists ttl ON tl.id = ttl.talent_list_id
        JOIN
            handy_casting.talents t ON ttl.talent_id = t.id
        LEFT JOIN
            handy_casting.talent_list_groups tlg ON ttl.talent_list_group_id = tlg.id
        WHERE 
            p.id = '{project_id}'
            AND tl.id = '{talent_list_id}'
        ORDER BY 
            tlg.name, ttl.sort_no;
            """
    
    result = execute_mysql_command(ssh_client, query)
    if result:
        lines = result.strip().split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]
        df = pd.DataFrame(data, columns=headers)
        
        # 数値型のカラムを特定
        numeric_columns = ['年齢', 'リスト価格', 'ソート番号']
        
        # null値をハイフンに置換（数値型のカラムは0をハイフンに）
        for column in df.columns:
            if column in numeric_columns:
                df[column] = df[column].replace({'0': '-', '': '-'})
            else:
                df[column] = df[column].fillna('-')
        
        # \nを改行として表示
        for column in df.columns:
            df[column] = df[column].apply(lambda x: x.replace('\\n', '\n') if isinstance(x, str) else x)
        
        return df
    return None

# タレントリスト名の選択肢を取得する関数
def get_talent_list_options(ssh_client, project_id):
    query = f"""
    SELECT tl.id, tl.name
    FROM handy_casting.talent_lists tl
    WHERE tl.project_id = '{project_id}'
    ORDER BY tl.id desc;
    """
    result = execute_mysql_command(ssh_client, query)
    if result:
        lines = result.strip().split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]
        return [(row[0], row[1]) for row in data]  # (id, name)のタプルのリストを返す
    return []

# product_genre_mapping.jsonから商品ジャンルの選択肢を取得する関数
def get_product_genre_options_from_json() -> List[str]:
    options = ['選択してください']  # デフォルトオプション
    options.extend(PRODUCT_GENRE_MAPPING.values())  # JSONファイルの順序を維持
    return options

# メイン関数
def main():
    st.title('AICSプロジェクト検索システム')
    st.info('サイドバーに条件を入力して検索ボタンを押してください。リストのグループ名や担当者やリスト記載の価格で検索もできます。過去の結果が残って気になるときはリロードしてください。')

    # セッション状態の初期化
    if 'search_params' not in st.session_state:
        st.session_state.search_params = {
            'project_name': '',
            'product_name': '',
            'talent_name': '',
            'product_genre': '選択してください',
            'group_name': '',
            'contact_person': '',
            'list_price': '',
            'created_start': None,
            'created_end': None,
            'created_start_enabled': False,
            'created_end_enabled': False
        }

    st.sidebar.header('検索条件')
    
    # 検索条件の入力
    st.session_state.search_params['project_name'] = st.sidebar.text_input('プロジェクト名', value=st.session_state.search_params['project_name'])
    st.session_state.search_params['product_name'] = st.sidebar.text_input('商品名', value=st.session_state.search_params['product_name'])
    
    # 商品ジャンルの選択肢を取得
    product_genre_options = get_product_genre_options_from_json()
    st.session_state.search_params['product_genre'] = st.sidebar.selectbox(
        '商品ジャンル',options=product_genre_options,
        index=product_genre_options.index(st.session_state.search_params['product_genre'])
    )

    # 新しい検索条件を追加
    st.session_state.search_params['talent_name'] = st.sidebar.text_input('タレント名', value=st.session_state.search_params['talent_name'])
    st.session_state.search_params['group_name'] = st.sidebar.text_input('リストグループ名', value=st.session_state.search_params['group_name'])
    st.session_state.search_params['list_price'] = st.sidebar.text_input('リスト価格(フリー入力)', value=st.session_state.search_params['list_price'])
    st.session_state.search_params['contact_person'] = st.sidebar.text_input('プロジェクト担当者', value=st.session_state.search_params['contact_person'])

    # 作成日（開始）の入力
    start_date_enabled = st.sidebar.checkbox("作成日（開始）を指定", value=st.session_state.search_params['created_start_enabled'])
    st.session_state.search_params['created_start_enabled'] = start_date_enabled

    if start_date_enabled:
        st.session_state.search_params['created_start'] = st.sidebar.date_input(
            '作成日（開始）',
            value=st.session_state.search_params['created_start'] or date.today(),
            key='created_start_input'
        )
    else:
        st.session_state.search_params['created_start'] = None

    # 作成日（終了）の入力
    end_date_enabled = st.sidebar.checkbox("作成日（終了）を指定", value=st.session_state.search_params['created_end_enabled'])
    st.session_state.search_params['created_end_enabled'] = end_date_enabled

    if end_date_enabled:
        st.session_state.search_params['created_end'] = st.sidebar.date_input(
            '作成日（終了）',
            value=st.session_state.search_params['created_end'] or date.today(),
            key='created_end_input'
        )
    else:
        st.session_state.search_params['created_end'] = None

    # 検索ボタンとリセットボタンを横に並べる
    col1, col2 = st.sidebar.columns(2)
    search_button = col1.button('検索')
    reset_button = col2.button('リセット')

    if reset_button:
        st.session_state.search_params = {
            'project_name': '',
            'product_name': '',
            'talent_name': '',
            'product_genre': '選択してください',
            'group_name': '',
            'contact_person': '',
            'list_price': '',
            'created_start': None,
            'created_end': None,
            'created_start_enabled': False,
            'created_end_enabled': False
        }
        st.rerun()

    if 'projects_df' not in st.session_state:
        st.session_state.projects_df = None

    if 'selected_project_id' not in st.session_state:
        st.session_state.selected_project_id = None

    if 'selected_talent_list_id' not in st.session_state:
        st.session_state.selected_talent_list_id = None

    if 'talents_df' not in st.session_state:
        st.session_state.talents_df = None

    if search_button:
        ssh_client, temp_key_file_path = create_ssh_client()
        if ssh_client:
            try:
                with st.spinner('プロジェクトを検索中...'):
                    st.session_state.projects_df = get_project_data(
                        ssh_client, 
                        st.session_state.search_params['project_name'], 
                        st.session_state.search_params['product_name'], 
                        st.session_state.search_params['talent_name'], 
                        st.session_state.search_params['product_genre'] if st.session_state.search_params['product_genre'] != '選択してください' else None, 
                        st.session_state.search_params['group_name'], 
                        st.session_state.search_params['contact_person'], 
                        st.session_state.search_params['list_price'],
                        st.session_state.search_params['created_start'] if st.session_state.search_params['created_start_enabled'] else None,
                        st.session_state.search_params['created_end'] if st.session_state.search_params['created_end_enabled'] else None
                    )
                
                if st.session_state.projects_df is not None and not st.session_state.projects_df.empty:
                    st.success(f'検索結果: {len(st.session_state.projects_df)}件のプロジェクトが見つかりました')
                else:
                    st.warning('検索条件に一致するプロジェクトが見つかりませんでした。')
            finally:
                ssh_client.close()
                os.unlink(temp_key_file_path)

    if st.session_state.projects_df is not None and not st.session_state.projects_df.empty:
        # プロジェクトの選択
        st.session_state.projects_df['プロジェクト表示名'] = st.session_state.projects_df.apply(
            lambda row: f"{row['プロジェクト名']}（{row['商品名']}）- {row['プロジェクト作成日']}", axis=1
        )
        project_options = st.session_state.projects_df['プロジェクト表示名'].tolist()
        selected_project = st.selectbox('プロジェクトを選択してください:', project_options)
        
        if selected_project:
            st.session_state.selected_project_id = st.session_state.projects_df[st.session_state.projects_df['プロジェクト表示名'] == selected_project]['プロジェクトID'].values[0]

    if st.session_state.selected_project_id:
        ssh_client, temp_key_file_path = create_ssh_client()
        if ssh_client:
            try:
                # 選択されたプロジェクトの詳細を表示
                st.subheader('プロジェクト詳細')
                try:
                    project_details = st.session_state.projects_df[st.session_state.projects_df['プロジェクトID'] == st.session_state.selected_project_id].iloc[0]
                    st.table(project_details.to_frame().T)
                except Exception as e:
                    st.error(f"プロジェクト詳細の表示中にエラーが発生しました: {str(e)}")

                # タレントリスト名の選択肢を取得
                talent_list_options = get_talent_list_options(ssh_client, st.session_state.selected_project_id)
                if talent_list_options:
                    talent_list_choices = [f"{name} (ID: {id})" for id, name in talent_list_options]
                    selected_talent_list = st.selectbox('タレントリストを選択してください:', talent_list_choices)
                    if selected_talent_list:
                        st.session_state.selected_talent_list_id = selected_talent_list.split("(ID: ")[1].split(")")[0]
                else:
                    st.warning('このプロジェクトに関連するタレントリストが見つかりませんでした。')
                    st.session_state.selected_talent_list_id = None

                # タレント情報の取得と表示
                if st.session_state.selected_talent_list_id:
                    with st.spinner('タレント情報を取得中...'):
                        st.session_state.talents_df = get_talent_data(ssh_client, st.session_state.selected_project_id, st.session_state.selected_talent_list_id)
                    
                    if st.session_state.talents_df is not None and not st.session_state.talents_df.empty:
                        st.subheader('タレント情報')
                        st.dataframe(st.session_state.talents_df)
                        
                        # タレント情報のCSV出力機能
                        csv = st.session_state.talents_df.to_csv(index=False)
                        st.download_button(
                            label="タレント情報をCSVでダウンロード",
                            data=csv,
                            file_name=f"{selected_project}_{st.session_state.selected_talent_list_id}_talents.csv",
                            mime="text/csv",
                        )
                    else:
                        st.warning('選択されたタレントリストに関連するタレント情報が見つかりませんでした。')
            except Exception as e:
                st.error(f"エラーが発生しました: {str(e)}")
            finally:
                ssh_client.close()
                os.unlink(temp_key_file_path)
                st.sidebar.info("EC2サーバーとの接続を閉じました。")

    if st.session_state.projects_df is not None and not st.session_state.projects_df.empty:
        # プロジェクト一覧のCSV出力機能
        csv = st.session_state.projects_df.to_csv(index=False)
        st.download_button(
            label="プロジェクト一覧をCSVでダウンロード",
            data=csv,
            file_name="project_search_results.csv",
            mime="text/csv",
        )

if __name__ == '__main__':
    main()