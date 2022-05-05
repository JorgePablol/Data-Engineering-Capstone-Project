sellout = """
    select daily, sum(pos_qty::float) as pos_qty, sum(pos_sales::float) as pos_sales, count(*) as scrapper_rows
    from sellout_sellout
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

sellout_wm_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(pos_sales::float) as pos_sales
    from sellout_wm_week_selloutwmweek
    where historical_execution_result_id in 
         (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_end_date <= %s)
                                                     or (her.report_end_date >= %s and her.report_end_date <= %s)))
    group by daily;
"""

sellout_iso_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(pos_sales::float) as pos_sales
    from sellout_iso_week_selloutisoweek
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

sellout_soriana_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(pos_sales::float) as pos_sales
    from sellout_soriana_week_sellout_soriana_week
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

inventory = """
    select daily, sum(curr_on_hand_qty::float) as curr_on_hand_qty, count(*) as scrapper_rows
    from inventory_inventory
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

inventory_wm_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(curr_on_hand_qty::float) as curr_on_hand_qty
    from inventory_wm_week_inventorywmweek
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

inventory_iso_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(curr_on_hand_qty::float) as curr_on_hand_qty
    from inventory_iso_week_inventoryisoweek
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""

inventory_soriana_week = """
    select daily, sum(pos_qty::float) as pos_qty, sum(curr_on_hand_qty::float) as curr_on_hand_qty
    from inventory_soriana_week_inventorysorianaweek
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by daily;
"""


pipeline = """
    select 
        historical_execution_result_id, 
        daily, 
        sum(pos_qty::float) as pos_qty, 
        sum(pos_sales::float) as pos_sales, 
        sum(curr_on_hand_qty::float) as curr_on_hand_qty, 
        sum(curr_in_transit_qty::float) as curr_in_transit, 
        sum(on_order::float) as on_order, 
        sum(curr_str_in_whse_qty::float) as wharehouse
        
    from pipeline_pipeline
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

    group by historical_execution_result_id, daily;
"""