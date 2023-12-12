with 

source as (

    select * from {{ source('fastfleet_googleads', 'Keyword_4201378109') }}

),

renamed as (

    select
        criterionid,
        adgroupid,
        campaignid,
        externalcustomerid,
        approvalstatus,
        cpcbid,
        cpcbidsource,
        cpmbidstr,
        finalmobileurls,
        finalurlsuffix,
        finalurls,
        keywordmatchtype,
        criteria,
        isnegative,
        estimatedaddclicksatfirstpositioncpc,
        estimatedaddcostatfirstpositioncpc,
        firstpagecpc,
        firstpositioncpc,
        topofpagecpc,
        creativequalityscore,
        postclickqualityscore,
        qualityscore,
        searchpredictedctr,
        status,
        systemservingstatus,
        verticalid,
        trackingurltemplate,
        urlcustomparameters,
        biddingstrategyid,
        biddingstrategytype,
        enhancedcpcenabled,
        biddingstrategyname,
        biddingstrategysource,
        criteriadestinationurl,
        finalappurls,
        hasqualityscore,
        labelids,
        labels,
        _latest_date,
        _data_date

    from source

)

select * from source
