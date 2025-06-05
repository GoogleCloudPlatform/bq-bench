select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 810 and 810+40
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and
        (i_color = 'gainsboro' or i_color = 'grey') and
        (i_units = 'Oz' or i_units = 'Lb') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'indian' or i_color = 'dodger') and
        (i_units = 'Gross' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'dim') and
        (i_units = 'Dram' or i_units = 'Dozen') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'deep' or i_color = 'dark') and
        (i_units = 'Box' or i_units = 'Case') and
        (i_size = 'petite' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and
        (i_color = 'drab' or i_color = 'saddle') and
        (i_units = 'Bunch' or i_units = 'Tsp') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'cornflower' or i_color = 'misty') and
        (i_units = 'Ounce' or i_units = 'Pound') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'smoke' or i_color = 'peru') and
        (i_units = 'Unknown' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'magenta' or i_color = 'plum') and
        (i_units = 'Pallet' or i_units = 'Tbl') and
        (i_size = 'petite' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;