import sqlite3
import string
import secrets
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('table1.db')
cursor = conn.cursor()

#   ----------------------------------------------------------------------
def trigger_report():
    sql_script = """
            -- Create a temporary table to store the query results
            CREATE TEMP TABLE IF NOT EXISTS temp_output AS
            WITH StoreStatusCTE AS (
            SELECT
                ss.store_id_ss,
            CASE WHEN ss.status = 'active' THEN 1 ELSE 0 END AS is_active,
                ss.timestamp
            FROM
                store_status ss
            ),
            UptimeDowntimeCTE AS (
            SELECT
                mh.store_id_mh,
                mh.day,
                mh.start_time,
                mh.end_time,
            SUM(ss.is_active) AS total_active_periods
            FROM
                Menu_hours mh
            LEFT JOIN
                StoreStatusCTE ss
            ON
                mh.store_id_mh = ss.store_id_ss
            AND (
                (ss.timestamp BETWEEN datetime('now', '-1 hour') AND datetime('now')) OR
                (ss.timestamp BETWEEN datetime('now', '-1 day') AND datetime('now')) OR
                (ss.timestamp BETWEEN datetime('now', '-7 days') AND datetime('now'))
            )
            GROUP BY
                mh.store_id_mh, mh.day, mh.start_time, mh.end_time
        )
            SELECT
                mh.store_id_mh AS store_id,
            SUM(CASE WHEN mh.day = strftime('%w', 'now') THEN mh.total_active_periods ELSE 0 END) AS uptime_last_hour_in_minutes,
            (SUM(CASE WHEN mh.day = strftime('%w', 'now') THEN mh.total_active_periods ELSE 0 END) * 60) AS uptime_last_day_in_hours,
            (SUM(CASE WHEN mh.day BETWEEN (strftime('%w', 'now') - 6) AND (strftime('%w', 'now')) THEN mh.total_active_periods ELSE 0 END) * 60) AS uptime_last_week_in_hours,
            SUM(CASE WHEN mh.day = strftime('%w', 'now') THEN (strftime('%s', mh.end_time) - strftime('%s', mh.start_time)) ELSE 0 END) / 60 AS downtime_last_hour_in_minutes,
            (SUM(CASE WHEN mh.day = strftime('%w', 'now') THEN (strftime('%s', mh.end_time) - strftime('%s', mh.start_time)) ELSE 0 END) / 3600) AS downtime_last_day_in_hours,
            (SUM(CASE WHEN mh.day BETWEEN (strftime('%w', 'now') - 6) AND (strftime('%w', 'now')) THEN (strftime('%s', mh.end_time) - strftime('%s', mh.start_time)) ELSE 0 END) / 3600) AS downtime_last_week_in_hours
            FROM
                UptimeDowntimeCTE mh
            GROUP BY
                mh.store_id_mh;

        -- Create the 'output2' table if it doesn't exist
            CREATE TABLE IF NOT EXISTS output2 AS
            SELECT * FROM temp_output;

        -- Drop the temporary table
            DROP TABLE IF EXISTS temp_output;
        """


    cursor.executescript(sql_script)
    random_string = ''.join(secrets.choice(string.ascii_letters + string.digits) for i in range(10))


    return random_string

#   -------------------------------------------------------
def get_report(id):
    if not id:
        print("Running\n")
    else:
        df = pd.read_sql_query("SELECT * FROM output2",conn)

        df.to_excel("output_report.xlsx",index=False)
        print("Exported the data!")

#   ----------------------------------------------------------

def main():

    id = trigger_report()
    print("Report ID: ",id)
    get_report(id)
    conn.commit()
    conn.close()

#   ----------------------------------------------------------

if __name__ == "__main__":
    main()










