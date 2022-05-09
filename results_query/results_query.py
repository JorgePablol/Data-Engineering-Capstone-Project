results_query = ("""
    select c.client_id, p.provider_id, d.daily, sum(scrapper_pos_qty), sum(scrapper_pos_sales), sum(scrapper_curr_on_hand_qty), sum(scrapper_rows)

    from indicators i join daily d on i.daily_id = d.daily_id
        join providers p on p.provider_id = i.provider_id
        join clients c on c.client_id = i.client_id
    where execution_id = (select max(execution_id) from indicators)
    group by p.provider_id, c.client_id, d.daily
    order by client_id, provider_id, daily;
""")
