
Create or replace table model_data as
with step1 as (
select b.*, a.INDIVIDUAL_ORIGINAL_SKEY,a.ORGANIZATION_SKEY
from (select INSURED_INDIVIDUAL_ORIGINAL_SKEY as INDIVIDUAL_ORIGINAL_SKEY,ORGANIZATION_SKEY,
        CASE 
            WHEN UPPER (MP.PRODUCT_SOURCE_SYSTEM_NAME) = 'LIFEPRO' 
            THEN RTRIM(SUBSTRING(PF.POLICY_NUMBER,3,45))
            WHEN UPPER (MP.PRODUCT_SOURCE_SYSTEM_NAME) = 'ADD'
            THEN RTRIM(SUBSTRING(PF.POLICY_NUMBER,9,45))
            ELSE PF.POLICY_NUMBER END                               
            AS BASE_POLICY_NUMBER
    FROM S_POLICY.POLICY_WH PF 
        INNER JOIN S_CM.MEMBERS_WH MP
            ON PF.MEMBERS_PRODUCT_SKEY = MP.MEMBERS_PRODUCT_SKEY)  a
inner join 
(select *
from S_FINANCE.POLICY_EXPERIENCE_HIST
where HIST_HKEY in (
    select HIST_HKEY
    from S_FINANCE.POLICY_EXPERIENCE_HIST_INDEX
    where HIST_CURRENT_FLAG = 'Y'
    and HIST_VIEW_MODE = 'Pricing'
    and HIST_LAG_MONTHS = 3
)
and PRODUCT_GROUP = 'PROD1'
) b 
on a.BASE_POLICY_NUMBER=b.POLICY
), 
step2 as (
select a.*, b.*
from
(select    INDIVIDUAL_ORIGINAL_SKEY as ind_skey,ORGANIZATION_SKEY as org_skey,
                age_at_issue,
                 annual_premium,
                 claim_date,
                 claim_payment_amt,
                 claim_payable_amt,
                 claim_paid_amt,
                 duration,
                 duration_start_date,
                 est_mortality_amt,
                 est_mortality_cnt,
                 expected_mortality_factor,
                 full_mortality_cnt,
                 gender,
                 issue_state,
                 mortality_exposure_amt,
                 mortality_exposure_cnt,
                 policy,
                 policy_start_date,
                 policy_coverage_amt,
                 retire_med,
                 underwriting_class,
                 underwriting_class_code,
                 value_per_coverage_unit,
                 exposure_cnt,channel,rate_factor_channel,rate_factor_media,lapse_flag,
                 rga_score,highest_drug_score,
             response_alcoholdrug as  alcoholdrug,
             response_brainspinal as  brainspinal,
             response_cancer as  cancer,
             response_depression as  depression,
             plan_code,
media_1,
media_2,
media_3,
channel_1,
channel_2,
channel_3,
channel_4,
coverage_band,
rx
                 from 
step1
 ) a
 inner join 
(select 
DATE(ETL_BEGIN_TS) as ETL_date,
      individual_original_SKey,
      organization_skey,
      household_skey,
      campaign_group,
      individual_data_enhancement_original_skey,
      contract,
      membersegmentcode,
      rtmi_member_nr,
      CU_STRATUM_SWL, CU_STRATUM_SWN,
      CASE WHEN
        COALESCE(CAST(addr_chg1_num1 as int), 0) +
        COALESCE(CAST(addr_chg2_num1 as int), 0) +
        COALESCE(CAST(addr_chg3_num1 as int), 0) +
        COALESCE(CAST(addr_chg4_num1 as int), 0) +
        COALESCE(CAST(addr_chg1_num2 as int), 0) +
        COALESCE(CAST(addr_chg2_num2 as int), 0) +
        COALESCE(CAST(addr_chg3_num2 as int), 0) +
        COALESCE(CAST(addr_chg4_num2 as int), 0) > 0 then 1 else 0
      END AS addrchg,
      adltfememppct,
      COALESCE(adultu_2534, 0) * 1 AS adultu_2534,
      advenergyriskscore,
      agencyfirstmortbalamt,
      agencyfirstmorthhpercent,
      allmailalltimecnt,
      allmailltalltimecnt,
      allmailtenrmonthnum,
      CASE
        WHEN auto_info_code = 'D' THEN 1
        ELSE 0
      END as auto_info_code,
      auto_mailing_number,
      auto_quality_score,
      CASE
        WHEN autoexpmonth = 1 THEN 'jan'
        WHEN autoexpmonth = 2 THEN 'feb'
        WHEN autoexpmonth = 3 THEN 'mar'
        WHEN autoexpmonth = 4 THEN 'apr'
        WHEN autoexpmonth = 5 THEN 'may'
        WHEN autoexpmonth = 6 THEN 'jun'
        WHEN autoexpmonth = 7 THEN 'jul'
        WHEN autoexpmonth = 8 THEN 'aug'
        WHEN autoexpmonth = 9 THEN 'sep'
        WHEN autoexpmonth = 10 THEN 'oct'
        WHEN autoexpmonth = 11 THEN 'nov'
        WHEN autoexpmonth = 12 THEN 'dec'
        ELSE NULL
      END AS autoexpmonth,
      COALESCE((autoexpmonth - substr(campaign_group, 5, 2)), -99999) AS autoexpmonth_diff,
      autofinpredictorscore,
      autolibertytenureinmonthsnumber,
      autolmalltimecnt,
      CASE
        WHEN automail_last90days = 'Y' THEN 1
        ELSE 0
      END AS automail_last90days,
      automailtenrmonthnum,
      CASE
        WHEN availhomeequity = 'A' THEN 1
        WHEN availhomeequity = 'B' THEN 2500
        WHEN availhomeequity = 'C' THEN 7500
        WHEN availhomeequity = 'D' THEN 15000
        WHEN availhomeequity = 'E' THEN 25000
        WHEN availhomeequity = 'F' THEN 40000
        WHEN availhomeequity = 'G' THEN 62500
        WHEN availhomeequity = 'H' THEN 87500
        ELSE 0
      END AS availhomeequity,
      avgfamincamt,
      bankautoloanhighcreamt,
      bankautoloannewnum,
      COALESCE(bibledevotreading * 1, 0) AS bibledevotereading,
      CASE
      WHEN billtype = 'List Bill' THEN 'LIST BILL'
      WHEN billtype = 'Direct Bill' THEN 'DIRECT BILL'
      END AS billtype,
      CASE
      WHEN billtype = 'List Bill' THEN CAST(OrganizationPPFSegment as STRING)
      WHEN billtype = 'Direct Bill' THEN NULL
      END AS organizationppfsegment,
      bnkrptcynvgtrscore,
      COALESCE(cashtransind * 1, 0) AS cashtransind,
      COALESCE(childages0_10n * 1, 0) AS childages0_10n,
      COALESCE(childhhcount * 1, -10) AS childhhcount,
      cnsmrpromind * 1 AS cnsmrpromind,
      COALESCE(collegecompflag * 1, 0) AS collegecompflag,
      COALESCE(communitycharitiesind * 1, 0) AS communitycharitiesind,
      CASE
        WHEN contrib_flag = 'Y' THEN 1
        ELSE 0
      END AS contrib_flag,
      countryoforigincode,
      COALESCE(crecardbuyerunktypeflag * 1, 0) AS crecardbuyerunktypeflag,
      COALESCE(creditcardbankflag * 1, 0) AS creditcardbankflag,
      COALESCE(creditcardupscaleflag * 1, 0) AS creditcardupscaleflag,
      CASE
        WHEN cust_pros_flag = 'C' THEN 1
        ELSE 0
      END AS cust_pros_flag,
      duplexpct,
      econstabindfincd * 1 AS econstabindfincd,
      educ1stind,
      educ1stindinf,
      educ2ndind,
      COALESCE(educonlineind * 1, 0) AS educonlineind,
      CASE
        WHEN eligage < 116 THEN eligage
        ELSE 116 -- There is no living human being older than 116 yrs old
      END AS eligage,
      equifaxriskscore,
      CASE -- IB008671_ESTIMATED_HH_INCOME_NARROW_RANGE_CODE_TX
        WHEN estincnarrowrangescd = '1' THEN 7500
        WHEN estincnarrowrangescd = '2' THEN 17500
        WHEN estincnarrowrangescd = '3' THEN 25000
        WHEN estincnarrowrangescd = '4' THEN 35000
        WHEN estincnarrowrangescd = '5' THEN 45000
        ELSE NULL
      END AS estincnarrowrangescd,
      CASE
        WHEN estincome * 1 = 1 THEN 7500
        WHEN estincome * 1 = 2 THEN 17500
        WHEN estincome * 1 = 3 THEN 25000
        WHEN estincome * 1 = 4 THEN 35000
        WHEN estincome * 1 = 5 THEN 45000
        WHEN estincome * 1 = 6 THEN 62500
        WHEN estincome * 1 = 7 THEN 87500
        WHEN estincome * 1 = 8 THEN 112500
        WHEN estincome * 1 = 9 THEN 150000
        ELSE NULL
      END AS estincome,
      CASE
        WHEN estincomeinf * 1 = 1 THEN 7500
        WHEN estincomeinf * 1 = 2 THEN 17500
        WHEN estincomeinf * 1 = 3 THEN 25000
        WHEN estincomeinf * 1 = 4 THEN 35000
        WHEN estincomeinf * 1 = 5 THEN 45000
        WHEN estincomeinf * 1 = 6 THEN 62500
        WHEN estincomeinf * 1 = 7 THEN 87500
        WHEN estincomeinf * 1 = 8 THEN 112500
        WHEN estincomeinf * 1 = 9 THEN 150000
        ELSE NULL
      END AS estincomeinf,
      COALESCE(exrhlthgrpind * 1, 0) AS exrhlthgrpind,
      famnooneempl_pct,
      femchildhhpct,
      CASE
        WHEN firstindtxtgender = 'M' THEN 1
        ELSE 0
      END AS firstindtxtgender,
      gendermfu,
      COALESCE(heavytransactorscd * 1, 20) AS heavytransactorscd,
      hhrankcode *1 AS hhrankcode,
      COALESCE(hhspentonlineproductsamt * 1, 0) AS hhspentonlineproductsamt,
      hhwith3plusvehiclespercent,
      hispaniclangprefcode,
      CASE
        WHEN homeinsurancepurchasemon = '01' THEN 'jan'
        WHEN homeinsurancepurchasemon = '1'  THEN 'jan'
        WHEN homeinsurancepurchasemon = '02' THEN 'feb'
        WHEN homeinsurancepurchasemon = '2'  THEN 'feb'
        WHEN homeinsurancepurchasemon = '03' THEN 'mar'
        WHEN homeinsurancepurchasemon = '3'  THEN 'mar'
        WHEN homeinsurancepurchasemon = '04' THEN 'abr'
        WHEN homeinsurancepurchasemon = '4'  THEN 'abr'
        WHEN homeinsurancepurchasemon = '05' THEN 'may'
        WHEN homeinsurancepurchasemon = '5'  THEN 'may'
        ELSE NULL
      END AS homeinsurancepurchasemon,
      CASE
        WHEN homemarketvalue = 'A' THEN 12500
        WHEN homemarketvalue = 'B' THEN 37500
        WHEN homemarketvalue = 'C' THEN 62500
        WHEN homemarketvalue = 'D' THEN 87500
        WHEN homemarketvalue = 'E' THEN 112500
        WHEN homemarketvalue = 'F' THEN 137500
        WHEN homemarketvalue = 'G' THEN 162500
        ELSE NULL
      END AS homemarketvalue,
      CASE
        WHEN homeownerrent = 'O' THEN 1
        ELSE 0
      END AS homeownerrent,
      CASE
        WHEN homeownerrentinf = 'O' THEN 1
        ELSE 0
      END AS homeownerrentinf,
      COALESCE(homepc * 1, 0) AS homepc,
      COALESCE(householdspentofflineonprodsamnt, 0) AS householdspentofflineonprodsamnt,
      COALESCE(householdspentonprodsamnt, 0) AS householdspentonprodsamnt,
      COALESCE(inbound_calls2, 0) AS inbound_calls2,
      individualzipcode,
      CASE
        WHEN indivnewtodb = 'Y' THEN 1
        ELSE 0
      END AS indivnewtodb,
      CASE
        WHEN indivstate IN ('AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
                            'GA', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
                            'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND',
                            'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR',
                            'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT',
                            'WA', 'WI', 'WV', 'WY') THEN indivstate
        ELSE 'US'
      END AS indivstate,
      inqmiscperc,
      intdopnnew1stmrtlnnum,
      intdopnnew1stmrtlnpnum,
      COALESCE(invfinancegrpind * 1, 0) AS invfinancegrpind,
      COALESCE(knownvehowncnt * 1, 0) AS knownvehowncnt,
      lengthofres * 1 AS lengthofres,
      lengthofres2 * 1 AS lengthofres2,
      CASE
        WHEN mailresponder = 'R' THEN 1
        ELSE 0
      END AS mailresponder,
      CASE
        WHEN maritalnewlywedflag = 'Y' THEN 1
        ELSE 0
      END AS maritalnewlywedflag,
      CASE
        WHEN maritalstatus IN ('A', 'M') THEN 1
        ELSE 0
      END AS maritalstatus,
      CASE
        WHEN maritalstatusinf IN ('A', 'M') THEN 1
        ELSE 0
      END AS maritalstatusinf,
      marriedwithrelatedchildhhpercent,
      meantravelmintoworknum,
      COALESCE((CAST(substr(campaign_group, 1, 4) as int) - CAST(SUBSTR(movedate, 1, 4) AS integer)), -99999) AS moveage,
      COALESCE(nascarind * 1, 0) AS nascarind,
      CASE
        WHEN networthestr = '1' THEN 0
        WHEN networthestr = '2' THEN 2500
        WHEN networthestr = '3' THEN 7500
        WHEN networthestr = '4' THEN 17500
        WHEN networthestr = '5' THEN 37500
        WHEN networthestr = '6' THEN 75000
        WHEN networthestr = '7' THEN 175000
        WHEN networthestr = '8' THEN 375000
        WHEN networthestr = '9' THEN 750000
        WHEN networthestr = 'A' THEN 1500000
        WHEN networthestr = 'B' THEN 3000000
        ELSE NULL
      END AS networthestr,
      CASE
        WHEN newcarbuyer = 'Y' THEN 1
        ELSE 0
      END AS newcarbuyer,
      CASE
        WHEN non_contrib_flag = 'Y' THEN 1
        ELSE 0
      END AS non_contrib_flag,
      nonmortcreperc,
      COALESCE(numcalls, 0) AS numcalls,
      COALESCE(numcuraddc, 0) AS numcuraddc,
      COALESCE(numdaysaddc, 0) AS numdaysaddc,
      ocpt_group,
      otherhhpercent,
      COALESCE(outdoorgrpind * 1, 0) AS outdoorgrpind,
      COALESCE(petdogowner * 1, 0) AS petdogowner,
      CASE
        WHEN presofchildren = 'Y' THEN 1
        ELSE 0
      END AS presofchildren,
      CASE
        WHEN presofchildreninf = 'Y' THEN 1
        ELSE 0
      END AS presofchildreninf,
      COALESCE(presofgrandchildren * 1, 0) AS presofgrandchildren,
      CASE
        WHEN presofsenioradult IN ('Y') THEN 1
        ELSE 0
      END AS presofsenioradult,
      privateforprofitworkclasspercent,
      COALESCE(profoccupflag * 1, 0) AS profoccupflag,
      racecode,
      CASE
        WHEN realestateinv IN ('I') THEN 1
        ELSE 0
      END AS realestateinv,
      CASE
        WHEN sharedraftind='Y' THEN 1
        ELSE 0
      END AS sharedraftind,
      singleunitpercent,
      COALESCE(smokeind * 1, 0) AS smokeind,
      COALESCE(stocksbonds * 1, 0) AS stocksbonds,
      COALESCE(sweepstakescontests * 1, 0) AS sweepstakescontests,
      CASE
        WHEN trgtautofinvalscore = 'A1' THEN 760
        WHEN trgtautofinvalscore = 'A2' THEN 760
        WHEN trgtautofinvalscore = 'B1' THEN 720
        WHEN trgtautofinvalscore = 'B2' THEN 720
        ELSE NULL
      END AS trgtautofinvalscore,
      CASE
        WHEN trgthhvaluescorecode = 'A1' THEN 760
        WHEN trgthhvaluescorecode = 'A2' THEN 760
        WHEN trgthhvaluescorecode = 'B1' THEN 720
        WHEN trgthhvaluescorecode = 'B2' THEN 720
        ELSE NULL
      END AS trgthhvaluescorecode,
      usepbltrwrkpct,
      vantagescore,
      COALESCE((CAST(substr(campaign_group, 1, 4) as int) - vehicle1year), -99999) AS vehicle1age,
      vehicle1makenumeric,
      vehicle1modelnumeric,
      vehicle2modelalpha,
      whitehhpercent,
      wireless2000score,
      CASE
        WHEN Cust_Pros_Flag = 'C' THEN 'C'
        WHEN Cust_Pros_Flag = 'P' THEN 'P'
      END AS AC_Product_SKey
 from S_CM.SCO_EXTRACT
 ) b
 on a.ind_skey=b.INDIVIDUAL_ORIGINAL_SKEY and a.org_skey=b.ORGANIZATION_SKEY
 where b.ETL_DATE <= a.DURATION_START_DATE 
 ), step3 as (
  select *, ROW_NUMBER() OVER(PARTITION
                             by POLICY, DURATION ORDER by ETL_date DESC)
   as row_number
   from step2
) ,step4 as (  
   select * from step3 
    where row_number=1 )
select * from step4 ;

-- Due to memory limit of 4.0 gig to convert to R dataframe from Spark, the data is partitioned.

create or replace table model_data_part1 as
with temp as (
SELECT   ROW_NUMBER()
  OVER (order by policy, duration) as row_num, *
  FROM model_data
) select * from temp
where row_num < 100000;

create or replace table model_data_part2 as
with temp as (
SELECT   ROW_NUMBER()
  OVER (order by policy, duration) as row_num, *
  FROM model_data
) select * from temp
where row_num >= 100000;
