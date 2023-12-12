with 

source as (

    select * from {{ source('fastfleet_googleads', 'KeywordStats_4201378109') }}

),

renamed as (

    select
        criterionid,
        adgroupid,
        campaignid,
        externalcustomerid,
        baseadgroupid,
        basecampaignid,
        activeviewcpm,
        activeviewctr,
        activeviewimpressions,
        activeviewmeasurability,
        activeviewmeasurablecost,
        activeviewmeasurableimpressions,
        activeviewviewability,
        averagecost,
        averagecpc,
        averagecpm,
        clicks,
        conversions,
        conversionrate,
        conversionvalue,
        cost,
        costperconversion,
        costpercurrentmodelattributedconversion,
        ctr,
        currentmodelattributedconversions,
        currentmodelattributedconversionvalue,
        gmailforwards,
        gmailsaves,
        gmailsecondaryclicks,
        impressions,
        interactiontypes,
        interactionrate,
        interactions,
        valueperconversion,
        valuepercurrentmodelattributedconversion,
        adnetworktype2,
        clicktype,
        date,
        dayofweek,
        device,
        month,
        quarter,
        week,
        year,
        adnetworktype1,
        averageposition,
        monthofyear,
        slot,
        _latest_date,
        _data_date

    from source

)

select * from source
