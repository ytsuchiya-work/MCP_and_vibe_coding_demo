import streamlit as st
import pandas as pd
from databricks import sql
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import time

# 環境変数を読み込み
load_dotenv('env.dbx-sql')

class DatabricksConnector:
    def __init__(self):
        self.server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        self.http_path = os.getenv('DATABRICKS_HTTP_PATH')
        self.access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        
    def get_connection(self):
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )
    
    def get_order_info(self, order_id):
        """注文情報を取得"""
        query = """
        SELECT 
            order_id,
            order_date,
            customer_id,
            origin_location_id,
            destination_location_id,
            service_level,
            order_value,
            weight_kg,
            status,
            created_at,
            updated_at
        FROM hiroshi.mcp.orders
        WHERE order_id = ?
        """
        
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (order_id,))
                result = cursor.fetchone()
                if result is not None and cursor.description is not None:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                return None
    
    def get_shipment_stops(self, order_id):
        """配送停車点情報を取得"""
        query = """
        SELECT 
            s.stop_id,
            s.order_id,
            s.stop_sequence,
            s.facility_id,
            f.facility_name,
            f.facility_type,
            f.city,
            f.region,
            s.planned_arrival_at,
            s.actual_arrival_at,
            s.planned_depart_at,
            s.actual_depart_at,
            s.delay_reason_code,
            s.created_at,
            s.updated_at
        FROM hiroshi.mcp.shipment_stops s
        JOIN hiroshi.mcp.facilities f ON s.facility_id = f.facility_id
        WHERE s.order_id = ?
        ORDER BY s.stop_sequence
        """
        
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (order_id,))
                results = cursor.fetchall()
                if results is not None and len(results) > 0 and cursor.description is not None:
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in results]
                return []

def format_datetime(dt):
    """タイムスタンプを日本時間で表示"""
    if dt is None:
        return "未定"
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_status_color(status):
    """ステータスに応じた色を返す"""
    color_map = {
        'pending': '#FFA500',
        'processing': '#1E90FF',
        'shipped': '#32CD32',
        'delivered': '#228B22',
        'cancelled': '#DC143C'
    }
    return color_map.get(status, '#808080')

def get_stop_status(stop):
    """停車点のステータスを判定"""
    if stop['actual_arrival_at'] and stop['actual_depart_at']:
        return "完了", "🟢"
    elif stop['actual_arrival_at']:
        return "到着済み", "🟡"
    elif stop['planned_arrival_at']:
        # タイムゾーン情報を考慮した比較
        current_time = datetime.now(timezone.utc)
        planned_time = stop['planned_arrival_at']
        
        # planned_timeがナイーブな時刻の場合、UTCとして扱う
        if planned_time.tzinfo is None:
            planned_time = planned_time.replace(tzinfo=timezone.utc)
        
        if current_time > planned_time:
            return "遅延", "🔴"
        else:
            return "予定", "⚪"
    else:
        return "未定", "⚫"

def main():
    st.set_page_config(
        page_title="配送状況確認システム",
        page_icon="📦",
        layout="wide"
    )
    
    st.title("📦 配送状況確認システム")
    st.markdown("---")
    
    # サイドバーで注文ID入力
    with st.sidebar:
        st.header("🔍 注文検索")
        order_id = st.text_input(
            "注文ID を入力してください",
            placeholder="例: 20250115-001",
            help="YYYYMMDD-NNN形式で入力してください"
        )
        
        search_button = st.button("検索", type="primary", use_container_width=True)
    
    # データベース接続
    db = DatabricksConnector()
    
    if search_button and order_id:
        with st.spinner("データを取得しています..."):
            # 注文情報を取得
            order_info = db.get_order_info(order_id)
            
            if order_info:
                # 配送停車点情報を取得
                shipment_stops = db.get_shipment_stops(order_id)
                
                # 注文情報を表示
                st.header(f"📋 注文情報: {order_id}")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("顧客ID", order_info['customer_id'])
                    st.metric("注文日時", format_datetime(order_info['order_date']))
                
                with col2:
                    st.metric("サービスレベル", order_info['service_level'])
                    st.metric("注文金額", f"¥{order_info['order_value']:,.0f}")
                
                with col3:
                    status_color = get_status_color(order_info['status'])
                    st.markdown(f"**ステータス:** <span style='color: {status_color}'>{order_info['status']}</span>", unsafe_allow_html=True)
                    st.metric("重量", f"{order_info['weight_kg']} kg")
                
                st.markdown("---")
                
                # 配送進捗を表示
                st.header("🚚 配送進捗")
                
                if shipment_stops:
                    # 進捗バーを表示
                    completed_stops = sum(1 for stop in shipment_stops if stop['actual_arrival_at'] and stop['actual_depart_at'])
                    total_stops = len(shipment_stops)
                    progress = completed_stops / total_stops if total_stops > 0 else 0
                    
                    st.progress(progress)
                    st.write(f"進捗: {completed_stops}/{total_stops} ステップ完了")
                    
                    # 配送ルートを表示
                    for i, stop in enumerate(shipment_stops):
                        status, emoji = get_stop_status(stop)
                        
                        with st.expander(f"{emoji} ステップ {stop['stop_sequence']}: {stop['facility_name']} ({status})", expanded=True):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write(f"**施設情報:**")
                                st.write(f"- 施設ID: {stop['facility_id']}")
                                st.write(f"- 施設タイプ: {stop['facility_type']}")
                                st.write(f"- 所在地: {stop['city']}, {stop['region']}")
                            
                            with col2:
                                st.write(f"**タイムスタンプ:**")
                                st.write(f"- 予定到着: {format_datetime(stop['planned_arrival_at'])}")
                                st.write(f"- 実際到着: {format_datetime(stop['actual_arrival_at'])}")
                                st.write(f"- 予定出発: {format_datetime(stop['planned_depart_at'])}")
                                st.write(f"- 実際出発: {format_datetime(stop['actual_depart_at'])}")
                                
                                if stop['delay_reason_code']:
                                    st.warning(f"⚠️ 遅延理由: {stop['delay_reason_code']}")
                else:
                    st.info("この注文の配送情報はまだ登録されていません。")
                
                # 詳細情報をテーブルで表示
                with st.expander("📊 詳細データ"):
                    st.subheader("注文詳細")
                    order_df = pd.DataFrame([order_info])
                    st.dataframe(order_df, use_container_width=True)
                    
                    if shipment_stops:
                        st.subheader("配送停車点詳細")
                        stops_df = pd.DataFrame(shipment_stops)
                        st.dataframe(stops_df, use_container_width=True)
                
            else:
                st.error(f"注文ID '{order_id}' が見つかりません。")
                st.info("注文IDを確認して再度検索してください。")
    
    elif search_button and not order_id:
        st.warning("注文IDを入力してください。")
    
    # フッター
    st.markdown("---")
    st.markdown("💡 **使用方法:** 左のサイドバーに注文IDを入力して検索ボタンを押してください。")

if __name__ == "__main__":
    main() 