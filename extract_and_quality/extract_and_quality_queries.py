sellout = ("""
    select daily, pos_qty, pos_sales
    from sellout_sellout
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))

""")

inventory = ("""
    select daily, curr_on_hand_qty
    from inventory_inventory
    where historical_execution_result_id in
            (select id from historical_execution_results_historicalexecutionresults as her
            where her.config_report_id = %s and ((her.report_start_date >= %s and her.report_start_date <= %s)
                                                or (her.report_end_date >= %s and her.report_end_date <= %s)))
""")

quality_checks = ("""
select d.daily, sum(scrapper_pos_qty), sum(scrapper_pos_sales), sum(scrapper_curr_on_hand_qty), sum(scrapper_rows)
        from indicators i join daily d on i.daily_id = d.daily_id
        where execution_id = (select max(execution_id) from indicators)
        group by d.daily;
""")
