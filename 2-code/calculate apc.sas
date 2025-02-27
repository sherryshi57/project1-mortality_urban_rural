
%let pg1dataset=E:\github\project 1\1-dataset; /* Set the file path folder to your own file path */
%let pg1output=E:\github\project 1\3-output;  /* Set the file path folder to your own file path */

*set a libref pg1;
libname pg1 "&pg1dataset";


data poisson_data;
	retain PopSize AgeGroup Year_num Deaths Population;
    set pg1.data_original2010_2022;
	* character to numberic;
    Year_num = input(Year, 4.);
	keep PopSize AgeGroup Year_num Deaths Population;
run;
proc means data=poisson_data mean std;
    var deaths Population; 
run;

proc sql;
    create table poisson_data_sum as
    select PopSize, AgeGroup, Year_num, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from poisson_data
	
    group by PopSize, AgeGroup, Year_num
    order by PopSize, AgeGroup, Year_num;
quit;

data pg1.poisson_data;
    set poisson_data_sum;
    LogPop = log(Population_sum);
run;


*Calculate annual percent change (APC) & p-value ;

/* Data Preparation for 2019 */
data pg1.poisson_data_2019;
    set pg1.poisson_data;
    where year_num not in (2020 2021 2022);
run;

%macro run_genmod(data, output_prefix, age_group_condition);
    proc genmod data=&data;
        class PopSize(ref='Large Metro') AgeGroup;
        model deaths_sum = PopSize AgeGroup Year_num PopSize*Year_num / dist = poisson
                           link = log offset = logpop pscale;
        estimate 'Large Metro' Year_num 1 Year_num*PopSize 0 0 1;
        estimate 'Small/Medium Metro' Year_num 1 Year_num*PopSize 0 1 0;
        estimate 'Rural' Year_num 1 Year_num*PopSize 1 0 0;
        ods output ParameterEstimates=&output_prefix._pvalue(keep = Parameter Level1 ProbChiSq) 
                    Estimates=&output_prefix._apc(keep = Label MeanEstimate MeanLowerCL MeanUpperCL);
    run;

    /* Process p-values */
    data &output_prefix._pvalue;
        retain Label ProbChiSq; 
        set &output_prefix._pvalue;
        where Parameter = 'Year_num*PopSize';
        Label = Level1;
        keep Label ProbChiSq; 
    run;
    
    proc sort data=&output_prefix._pvalue; by Label; run;
    proc sort data=&output_prefix._apc; by Label; run;

    /* Merge Results */
    data merged_&output_prefix.; 
        retain Label Category;
        merge &output_prefix._apc(in=a) &output_prefix._pvalue(in=b);
        by Label;
        if a or b;
        Category="&output_prefix.";
        format MeanEstimate MeanLowerCL MeanUpperCL percent8.2; 
    run;
%mend;

%run_genmod(pg1.poisson_data_2019, overall2019);
%run_genmod(pg1.poisson_data_2019(where=(AgeGroup not in ('65-74', '75-84', '85+'))), age2564_2019, AgeGroup not in ('65-74' '75-84' '85+'));
%run_genmod(pg1.poisson_data_2019(where=(AgeGroup in ('65-74', '75-84', '85+'))), age65_2019, AgeGroup in ('65-74' '75-84' '85+'));

/* Data Preparation for 2022 */
data pg1.poisson_data_2022;
    set pg1.poisson_data;
    where year_num in (2020 2021 2022);
run;

%run_genmod(pg1.poisson_data_2022, overall2022);
%run_genmod(pg1.poisson_data_2022(where=(AgeGroup not in ('65-74', '75-84', '85+'))), age2564_2022, AgeGroup not in ('65-74' '75-84' '85+'));
%run_genmod(pg1.poisson_data_2022(where=(AgeGroup in ('65-74', '75-84', '85+'))), age65_2022, AgeGroup in ('65-74' '75-84' '85+'));


%macro create_dataset(in_data=, out_data=, est_prefix=, est_suffix=);
    data &out_data;
        set &in_data;
        Estimate_&est_suffix = catx('', put(MeanEstimate, percent8.2), '(', put(MeanLowerCL, percent8.2), ',', put(MeanUpperCL, percent8.2), ')');
        keep Label Category Estimate_&est_suffix ProbChiSq;
    run;
%mend;

%create_dataset(in_data=merged_overall2019, out_data=merged_overall2019, est_prefix=2010, est_suffix=2010_2019);
%create_dataset(in_data=merged_age2564_2019, out_data=merged_age2564_2019, est_prefix=2010, est_suffix=2010_2019);
%create_dataset(in_data=merged_age65_2019, out_data=merged_age65_2019, est_prefix=2010, est_suffix=2010_2019);
%create_dataset(in_data=merged_overall2022, out_data=merged_overall2022, est_prefix=2020, est_suffix=2020_2022);
%create_dataset(in_data=merged_age2564_2022, out_data=merged_age2564_2022, est_prefix=2020, est_suffix=2020_2022);
%create_dataset(in_data=merged_age65_2022, out_data=merged_age65_2022, est_prefix=2020, est_suffix=2020_2022);


/* Macro to combine and sort datasets for each year */
%macro combine_sort_datasets(year);
    data combined_&year;
	    length Category $15;
        set merged_overall&year
            merged_age2564_&year
            merged_age65_&year;
        rename ProbChiSq= pvalue_&year;
    run;
    proc sort data=combined_&year; by Label; run;
%mend;

/* Run macro for each year */
%combine_sort_datasets(2019);
%combine_sort_datasets(2022);


/* Merge 2019 and 2022 datasets and display results */
data pg1.apc_total;
    length Category $15;
    retain Category PopSize Estimate_2010_2019 pvalue_2019 Estimate_2020_2022 pvalue_2022;

    /* Merge the datasets and rename Label to PopSize in the merging process */
    merge combined_2019 (in=a rename=(Category=Category_2019 Label=PopSize))
          combined_2022 (in=b rename=(Category=Category_2022 Label=PopSize));

    by PopSize;  /* Adjusted from 'Label' to 'PopSize' after the rename */

    /* Conditional check that both datasets must have the row to include it */
    if a & b;

    /* Conditional renaming based on category values from different years */
    if category_2019 = 'overall2019' or category_2022 = 'overall2022' then Category = 'Overall';
    else if category_2019 = 'age2564_2019' or category_2022 = 'age2564_2022' then Category = 'Age 25-64';
    else if category_2019 = 'age65_2019' or category_2022 = 'age65_2022' then Category = 'Age 65+';

    /* Keep only the specified variables */
    keep Category PopSize Estimate_2010_2019 pvalue_2019 Estimate_2020_2022 pvalue_2022;
run;



/* Sort the pg1.poisson_result dataset by category */
proc sort data=pg1.apc_total;
    by descending category;
run;


/* Display the merged data to verify */
proc print data=pg1.apc_total;
run;

%macro create_summary_table(category, condition);
    proc sql;
        create table poisson_n_&category as
        select "&category" as Category, PopSize, sum(deaths) format comma20. as Deaths_sum 
        from poisson_data
        where &condition
        group by PopSize
        union all
		select "&category" as Category, "&category" as PopSize, sum(deaths) as Deaths_sum 
        from poisson_data
        where &condition;
    quit;
%mend;

%create_summary_table(overall, condition=1=1);
%create_summary_table(age2564, AgeGroup not in ('65-74', '75-84', '85+'));
%create_summary_table(age65, AgeGroup in ('65-74', '75-84', '85+'));

proc sql;
    create table n_data as
    select 'Overall' as Category,PopSize, Deaths_sum as N format comma20. from poisson_n_overall
    union all
    select 'Age 25-64' as Category,PopSize, Deaths_sum as N from poisson_n_age2564
    union all
    select 'Age 65+' as Category,PopSize, Deaths_sum as N from poisson_n_age65;
quit;

/* create dataset: age-adjusted mortality rates (AAMRs) */
proc sql;
    create table aamr_data as
    select
        case
            when Category = 'overall' then 'Overall'
            when Category = 'age2564' then 'Age 25-64'
            when Category = 'age65+' then 'Age 65+'
            else Category
        end as Category,
        PopSize,
        _2010,
        _2019,
        _2022
    from pg1.final_data
    where Category not in ('Female', 'Male') /* Added comma to separate items in list */
    order by Category desc, PopSize;
quit;

proc sql;
    create table pg1.regression_out as
    select 
        coalesce(a.Category, b.Category, c.Category) as Category,
        coalesce(a.PopSize, b.PopSize, c.PopSize) as PopSize,
        b.N, 
        put(a._2010, $30.) as AAMR_2010 format $30., 
        put(a._2019, $30.) as AAMR_2019 format $30., 
        put(a._2022, $30.) as AAMR_2022 format $30.,
        put(c.Estimate_2010_2019, $30.) as APC_2010_2019 format $25.,
        c.pvalue_2019 format 6.4 as Interaction_p_2019,
        put(c.Estimate_2020_2022, $30.) as APC_2019_2022 format $25.,
        c.pvalue_2022 format 6.4 as Interaction_p_2022
    from aamr_data a
    full join n_data b
        on a.Category = b.Category and a.PopSize = b.PopSize
    full join pg1.apc_total c
        on coalesce(a.Category, b.Category) = c.Category 
        and coalesce(a.PopSize, b.PopSize) = c.PopSize
    order by 
        case 
            when Category = "Overall" then 1
            when Category = "Age 25-64" then 2
            when Category = "Age 65+" then 3
            else 4 
        end,
        PopSize;
quit;


* Adding Titles and Footnotes left justification;
title1 j=l "Table:Differences in Age-Standardized Cardiovascular Mortality by Urban-Rural classification,2022 versus 2010";
footnote1 j=l "Source: CDC Wonder";

* Define ODS Excel to output with specific Excel features;
ods excel file="&pg1output.\regression_out_data.xlsx"
    options(sheet_name="poisson regression" embedded_titles="yes" embedded_footnotes="yes");

* Example PROC PRINT or other reporting procedure to generate output;
proc print data=pg1.regression_out;
run;

ods excel close;
title;
footnote;

