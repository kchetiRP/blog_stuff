--rent blog post query
with dates as

(
    select  '20220101' as analysis_start_date
           ,'20220104' as analysis_end_date
)




,blog_landers_rent as
        (
        #blog lander sessions
        SELECT
        distinct
        PARSE_DATE("%Y%m%d", Date) as fmt_date
        -- ,case when page.pagepath LIKE '%?amp%' then 'mobile'
        --      else 'non-mobile'
        --      end mobile
        /*dont use this ^^ url stuff can change */
        ,case when isEntrance = True then 'FirstLander'
            when isEntrance = False then 'PriorGuest'
            end as firstLander_flag
        ,channelGrouping as subchannel
        ,case when channelGrouping='Organic Search' then 'Organic' when channelGrouping='Direct' then 'Organic' else 'Paid' end as Channel
        ,device.deviceCategory
        ,'Rent' as Brand
        ,geoNetwork.metro
        ,geoNetwork.city
        ,(CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID

        ,hitNumber as blogHit
        ,replace(
            replace( page.pagePathLevel2, '-',' ')
                                        ,'/', '')
                 as blog_title

        FROM `big-query-152314.82143969.ga_sessions_*` sessions
            ,UNNEST(sessions.hits) AS h


        --
        WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)

        --AND h.isEntrance=TRUE /*this filters out people who were here from another place in the website*/

        AND (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%') /*pagepath is a alias for url*/
        AND h.type='PAGE'

        group by 1,2,3,4,5,6,7,8,9,10,11

        --be sure to include dimensions:
            --user location, channel, subchannel, device, product, geonetwork.metro, geonetwork.city
        )

#all sessions with an srp, pdp, or home pageview, and that was not their landing page

--would need to do a left join on this with the blog landers query above to see what the next page type they went to was

,other_pages_rent as
            --what other pages did blog visitors visit?
            (SELECT
            distinct
            (CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID
            ,hitnumber
            ,cd10.value as pagetype

            --,min(hitNumber) as firstHit

            FROM `big-query-152314.82143969.ga_sessions_*` sessions
                ,UNNEST(sessions.hits) AS h
                ,UNNEST(h.customDimensions) AS cd10




            WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
            --AND (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%')
            /*question:^^this would not go here because  we are trying to see visits to other sights right*/
            AND h.type='PAGE'
            -- WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(),INTERVAL -5 DAY))

            AND cd10.index=10
            AND (cd10.value like '%srp%' or cd10.value like '%pdp%' or cd10.value like '%home%')
            /*include this filter to limit next page results to srp,pdp, and home which are the only ones relevant to this rquest*/
            --This filters only to srp,pdp and homepage
            -- there are more types of pages the visitor can visit
            AND h.type='PAGE'



            --need to add dimensions:
                --user location, channel, subchannel, device, product, geonetwork.metro, geonetwork.city
            )

--------------------------------------------------------------------------
#all leads (not just paid leads)  --do a left join w/ the blog landers query to only count leads for users who
,leads_rent as

                    (SELECT


                        (CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID

                        ,sum(
                            cast((SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=4) as int)
                            ) as email_lead
                        ,sum(
                            cast((SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=5) as int)
                            ) as phone_lead
                        ,sum(
                            cast( (SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=12) as int)
                            ) as core_email_lead
                        ,sum(
                            cast( (SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=13) as int)
                            ) as core_phone_lead
                        ,max(hitNumber) last_hit

                        --FROM `big-query-152314.930774.ga_sessions_*` sessions
                        FROM `big-query-152314.82143969.ga_sessions_*` sessions
                            ,UNNEST(sessions.hits) AS h
                            ,UNNEST(h.customDimensions) AS cd1
                            ,UNNEST(h.customMetrics) AS cm
                            --  ,UNNEST(h.customMetrics) AS cm5



                        WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
                        AND cd1.index=10
                        --   AND cm4.index=4
                        --   AND cm5.index=5
                        AND (cm.index=4 or cm.index=5 or cm.index=12 or cm.index=13)
                        AND (cm.value IS NOT NULL)

                        --AND h.type = 'EVENT'
                        group by 1
                        order by 1,2,3,4,5



                    )


,blog_time_rent as
                (

                SELECT
               CONCAT(fullvisitorid, FORMAT("%d", visitID)) as sessionid
                ,(MAX(h.time)/1000)-(MIN(h.time)/1000) as blog_dwell_time
                --,max(cd45.value) as ag_blog_time
                FROM `big-query-152314.82143969.ga_sessions_*` as a
               ,UNNEST(a.hits) AS h
                --,unnest(h.customDimensions) as cd45 --ag blog time


                WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
               and (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%')
               --and cd45.index= 45

                GROUP BY sessionid
                HAVING blog_dwell_time > 0
                )



,tsd_rent as

            (


            SELECT
            CONCAT(fullvisitorid, FORMAT("%d", visitID)) as sessionid
            ,(MAX(h.time)/1000) as sessionTime
            FROM `big-query-152314.82143969.ga_sessions_*` as a
                ,UNNEST(a.hits) AS h

            WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
            GROUP BY sessionid)





,blog_landers_AG as
        (
        #blog lander sessions
        SELECT
        distinct
        PARSE_DATE("%Y%m%d", Date) as fmt_date
        -- ,case when page.pagepath LIKE '%?amp%' then 'mobile'
        --      else 'non-mobile'
        --      end mobile
        /*dont use this ^^ url stuff can change */
        ,case when isEntrance = True then 'FirstLander'
            when isEntrance = False then 'PriorGuest'
            end as firstLander_flag
        ,channelGrouping as subchannel
        ,case when channelGrouping='Organic Search' then 'Organic' when channelGrouping='Direct' then 'Organic' else 'Paid' end as Channel
        ,device.deviceCategory
        ,'AG' as Brand
        ,geoNetwork.metro
        ,geoNetwork.city
        ,(CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID

        ,hitNumber as blogHit
        ,replace(
            replace( page.pagePathLevel2, '-',' ')
                                        ,'/', '')
                 as blog_title

        FROM `big-query-152314.930774.ga_sessions_*` sessions
            ,UNNEST(sessions.hits) AS h


        --
        WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)

        --AND h.isEntrance=TRUE /*this filters out people who were here from another place in the website*/

        AND (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%') /*pagepath is a alias for url*/
        AND h.type='PAGE'

        group by 1,2,3,4,5,6,7,8,9,10,11

        --be sure to include dimensions:
            --user location, channel, subchannel, device, product, geonetwork.metro, geonetwork.city
        )

#all sessions with an srp, pdp, or home pageview, and that was not their landing page

--would need to do a left join on this with the blog landers query above to see what the next page type they went to was

,other_pages_AG as
            --what other pages did blog visitors visit?
            (SELECT
            distinct
            (CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID
            ,hitnumber
            ,cd10.value as pagetype

            --,min(hitNumber) as firstHit

            FROM `big-query-152314.930774.ga_sessions_*` sessions
                ,UNNEST(sessions.hits) AS h
                ,UNNEST(h.customDimensions) AS cd10




            WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
            --AND (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%')
            /*question:^^this would not go here because  we are trying to see visits to other sights right*/
            AND h.type='PAGE'
            -- WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_ADD(CURAG_DATE(),INTERVAL -5 DAY))

            AND cd10.index=10
            AND (cd10.value like '%srp%' or cd10.value like '%pdp%' or cd10.value like '%home%')
            /*include this filter to limit next page results to srp,pdp, and home which are the only ones relevant to this rquest*/
            --This filters only to srp,pdp and homepage
            -- there are more types of pages the visitor can visit
            AND h.type='PAGE'



            --need to add dimensions:
                --user location, channel, subchannel, device, product, geonetwork.metro, geonetwork.city
            )

--------------------------------------------------------------------------
#all leads (not just paid leads)  --do a left join w/ the blog landers query to only count leads for users who
,leads_AG as

                    (SELECT


                        (CONCAT(fullvisitorid, FORMAT("%d", visitID))) AS SessionID

                        ,sum(
                            cast((SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=4) as int)
                            ) as email_lead
                        ,sum(
                            cast((SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=5) as int)
                            ) as phone_lead
                        ,sum(
                            cast( (SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=12) as int)
                            ) as core_email_lead
                        ,sum(
                            cast( (SELECT cm.value FROM h.customMetrics AS cm WHERE cm.index=13) as int)
                            ) as core_phone_lead
                        ,max(hitNumber) last_hit

                        --FROM `big-query-152314.930774.ga_sessions_*` sessions
                        FROM `big-query-152314.930774.ga_sessions_*` sessions
                            ,UNNEST(sessions.hits) AS h
                            ,UNNEST(h.customDimensions) AS cd1
                            ,UNNEST(h.customMetrics) AS cm
                            --  ,UNNEST(h.customMetrics) AS cm5



                        WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
                        AND cd1.index=10
                        --   AND cm4.index=4
                        --   AND cm5.index=5
                        AND (cm.index=4 or cm.index=5 or cm.index=12 or cm.index=13)
                        AND (cm.value IS NOT NULL)

                        --AND h.type = 'EVENT'
                        group by 1
                        order by 1,2,3,4,5



                    )


,blog_time_AG as
                (

                SELECT
               CONCAT(fullvisitorid, FORMAT("%d", visitID)) as sessionid
                ,(MAX(h.time)/1000)-(MIN(h.time)/1000) as blog_dwell_time
                --,max(cd45.value) as ag_blog_time
                FROM `big-query-152314.82143969.ga_sessions_*` as a
               ,UNNEST(a.hits) AS h
                --,unnest(h.customDimensions) as cd45 --ag blog time


                WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
               and (page.pagepath LIKE '%blog%' or page.pagepath LIKE '%research%')
               --and cd45.index= 45

                GROUP BY sessionid
                HAVING blog_dwell_time > 0
                )



,tsd_AG as

            (


            SELECT
            CONCAT(fullvisitorid, FORMAT("%d", visitID)) as sessionid
            ,(MAX(h.time)/1000) as sessionTime
            FROM `big-query-152314.82143969.ga_sessions_*` as a
                ,UNNEST(a.hits) AS h

            WHERE _TABLE_SUFFIX between (select analysis_start_date from dates) and (select analysis_end_date from dates)
            GROUP BY sessionid)
            --you would want to do an inner join with the the blog users to make sure to only count avg session duration for blog users


select  s.fmt_date date_of_b_session
        ,s.SessionID
        --should this be a flag or filter?
        ,s.firstLander_flag blog_first_page_flag
        ,s.subchannel
        ,s.Channel
        ,s.deviceCategory blog_reader_device
        ,s.Brand website
        ,s.metro metro_of_blog_reader
        ,s.city city_of_blog_reader
        ,s.blog_title



        --this column is very spotty, procede w/ caution
        , case when t.blog_dwell_time is null then tsd.sessionTime
               else t.blog_dwell_time
          end blog_session_time


          --leads
        ,email_lead  email_leads_after_blog
        ,phone_lead phone_leads_after_blog
        ,core_email_lead core_email_leads_after_blog
        ,core_phone_lead core_phone_leads_after_blog



         --next pages after blog
        ,max( case when op.pagetype like '%pdp%' then 1 else 0 end ) pdp_page_visit_f
        ,max( case when op.pagetype like '%srp%' then 1 else 0 end ) srp_page_visit_f
        ,max( case when op.pagetype like '%home%' then 1 else 0 end ) home_page_visit_f



from  blog_landers_rent s


left join other_pages_rent op
on  (s.sessionid = op.sessionid
/*ask brian if he is only interested in max*/
       and (s.bloghit ) < op.hitnumber )
left join  leads_rent l
on (l.SessionID = s.sessionid
         )
left join blog_time_rent t
 on s.sessionid = t.SessionID
left join tsd_rent tsd
on s.sessionid = tsd.sessionid
-- --shorten column names
-- where (phone_lead is not null
--      or email_lead is not null)
group by s.fmt_date
        ,s.SessionID
        --should this be a flag or filter?
        ,s.firstLander_flag
        ,s.subchannel
        ,s.Channel
        ,s.deviceCategory
        ,s.Brand
        ,s.metro
        ,s.city
        ,s.blog_title
        , case when t.blog_dwell_time is null then tsd.sessionTime
               else t.blog_dwell_time
          end
          ,email_lead
        ,phone_lead
        ,core_email_lead
        ,core_phone_lead


union all



select  ag.fmt_date date_of_b_session
        ,AG.SessionID
        --should this be a flag or filter?
        ,AG.firstLander_flag blog_first_page_flag
        ,AG.subchannel
        ,AG.Channel
        ,AG.deviceCategory blog_reader_device
        ,AG.Brand website
        ,AG.metro metro_of_blog_reader
        ,AG.city city_of_blog_reader
        ,AG.blog_title



        --this column is very spotty, procede w/ caution
        , case when ta.blog_dwell_time is null then tsda.sessionTime
               else ta.blog_dwell_time
          end blog_session_time


          --leads
        ,email_lead  email_leads_after_blog
        ,phone_lead phone_leads_after_blog
        ,core_email_lead core_email_leads_after_blog
        ,core_phone_lead core_phone_leads_after_blog



         --next pages after blog
        ,max( case when opa.pagetype like '%pdp%' then 1 else 0 end ) pdp_page_visit_f
        ,max( case when opa.pagetype like '%srp%' then 1 else 0 end ) srp_page_visit_f
        ,max( case when opa.pagetype like '%home%' then 1 else 0 end ) home_page_visit_f



from  blog_landers_AG ag


left join other_pages_AG opa
on  (AG.sessionid = opa.sessionid
/*ask brian if he is only interested in max*/
       and (AG.bloghit ) < opa.hitnumber )
left join  leads_AG la
on (la.SessionID = AG.sessionid  )
 left join blog_time_AG ta
 on AG.sessionid = ta.SessionID
left join tsd_AG tsda
on AG.sessionid = tsda.sessionid
-- --shorten column names
-- where (phone_lead is not null
--      or email_lead is not null)
group by AG.fmt_date
        ,AG.SessionID
        --should this be a flag or filter?
        ,AG.firstLander_flag
        ,AG.subchannel
        ,AG.Channel
        ,AG.deviceCategory
        ,AG.Brand
        ,AG.metro
        ,AG.city
        ,AG.blog_title
        , case when ta.blog_dwell_time is null then tsda.sessionTime
               else ta.blog_dwell_time
          end
        ,email_lead
        ,phone_lead
        ,core_email_lead
        ,core_phone_lead
