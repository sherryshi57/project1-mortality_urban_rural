
%let pg1dataset=E:\github\project 1\1-dataset; /* Set the file path folder to your own file path */
%let pg1output=E:\github\project 1\3-output;  /* Set the file path folder to your own file path */

*set a libref pg1;
libname pg1 "&pg1dataset";

***********************************************************;
* Data import and processing of 2010-2020 data  *;
***********************************************************;

*import txt data and scan max;
options validvarname=v7;
proc import datafile="&pg1dataset\Underlying Cause of Death, 2010-2020.txt"
            out=pg1.data1
            dbms=tab
            replace;
    guessingrows=max;
run;


data pg1.data1_new;
	retain PopSize AgeClass AgeGroup Gender Year Deaths Population;
	set pg1.data1;
    where notes is missing;
	*Year_Date = input(cats(Year_Code, '01', '01'), yymmdd10.);
    *format Year_Date year4.;
	*Gender = lowcase(Gender);
    length AgeClass $8 PopSize $20;
    AgeGroup = Ten_Year_Age_Groups_Code;
    if _2013_Urbanization_Code in (1 2) then popsize = 'Large Metro';
    else if _2013_Urbanization_Code in (3 4) then popsize = 'Small/Medium Metro';
    else if _2013_Urbanization_Code in (5 6) then popsize = 'Rural';
    *Use IF-THEN to add categorized columns for adults and elder persons;
    if missing(Ten_Year_Age_Groups_Code) then AgeClass = ' ';
    else if Ten_Year_Age_Groups_Code in ('25-34' '35-44' '45-54' '55-64') then AgeClass = 'age2564';
    else if Ten_Year_Age_Groups_Code in ('65-74' '75-84' '85+') then AgeClass = 'age65+';

	keep PopSize AgeClass AgeGroup Gender Year Deaths Population;
run;




***********************************************************;
* Data import and processing of 2020-2022 data    *;
***********************************************************;

options validvarname=v7;
proc import datafile="&pg1dataset\Underlying Cause of Death, 2020-2022, Single Race.txt"
            out=pg1.data2
            dbms=tab
            replace;
    guessingrows=max;
run;

data pg1.data2_new;
	retain PopSize AgeClass AgeGroup Gender Year Deaths Population;
	set pg1.data2;
	where notes is missing;
	Year_Date = input(cats(Year_Code, '01', '01'), yymmdd10.);
    format Year_Date year4.;
	*Gender = lowcase(Gender);
    length AgeClass $8 PopSize $20;
    
	/* Map Five_Year_Age_Groups_Code to new AgeGroup categories */
    if Five_Year_Age_Groups_Code in ('25-29', '30-34') then AgeGroup = '25-34';
    else if Five_Year_Age_Groups_Code in ('35-39', '40-44') then AgeGroup = '35-44';
    else if Five_Year_Age_Groups_Code in ('45-49', '50-54') then AgeGroup = '45-54';
    else if Five_Year_Age_Groups_Code in ('55-59', '60-64') then AgeGroup = '55-64';
    else if Five_Year_Age_Groups_Code in ('65-69', '70-74') then AgeGroup = '65-74';
    else if Five_Year_Age_Groups_Code in ('75-79', '80-84') then AgeGroup = '75-84';
    else if Five_Year_Age_Groups_Code in ('85-89', '90-94','95-99','100+') then AgeGroup = '85+';
    else AgeGroup = 'Unknown';

    /* Define AgeClass based on AgeGroup */
    if AgeGroup in ('25-34', '35-44', '45-54', '55-64') then AgeClass = 'age2564';
    else if AgeGroup in ('65-74', '75-84', '85+') then AgeClass = 'age65+';
    else AgeClass = 'Unknown';

    if _2013_Urbanization_Code in (1 2) then popsize = 'Large Metro';
    else if _2013_Urbanization_Code in (3 4) then popsize = 'Small/Medium Metro';
    else if _2013_Urbanization_Code in (5 6) then popsize = 'Rural';
    
	
	keep PopSize AgeClass AgeGroup Gender Year Deaths Population;
run;

proc sql;
    create table pg1.data2_sum as
    select PopSize, AgeClass, AgeGroup, Gender, Year, sum(deaths) as Deaths, 
        sum(input(population, best32.)) as Population
    from pg1.data2_new
    group by PopSize, AgeClass, AgeGroup, Gender, Year
    order by PopSize, AgeClass, AgeGroup, Gender, Year;
quit;


/* Check and fill data2_new missing value, 
use the values from data 1_new for the year 2020 */

proc sql;
    create table pg1.data2_missing as
    select *
    from pg1.data2_sum
    where Population is missing;
quit;

proc sql;
    create table pg1.data1_sum as
    select PopSize, AgeClass, AgeGroup, Gender, Year, sum(deaths) as Deaths_sum, 
        sum(population) as Population_sum
    from pg1.data1_new
    group by PopSize, AgeClass, AgeGroup, Gender, Year
    order by PopSize, AgeClass, AgeGroup, Gender, Year;
quit;

proc sql;
	create table pg1.fill_pop_missing as
	select PopSize, Gender, Population_sum as Population
	from pg1.data1_sum
	where AgeGroup eq '85+' and year eq '2020';
quit;

proc sql;
   create table pg1.data2_filled as
   select a.popsize, a.gender, a.year, 
          b.Population as Population
   from pg1.data2_missing as a
   left join pg1.fill_pop_missing as b
   on a.PopSize = b.PopSize and a.Gender = b.Gender;
quit;

proc sql;
    update pg1.data2_sum as original
    set Population = (select filled.Population
                      from pg1.data2_filled as filled
                      where filled.PopSize = original.PopSize
                        and filled.Gender = original.Gender
                        and filled.Year = original.Year)
    where original.Population is missing;
quit;

***********************************************************;
* Merge data 2010-2020 & 2020-2022 notice the year 2020   *;
***********************************************************;

proc sql;
    create table pg1.data_original2010_2022 as
    select * from pg1.data1_new
    union all
    select * from pg1.data2_sum
    where year ^= '2020';
quit;


***********************************************************;
* Data import and processing of reference population data *;
***********************************************************;

options validvarname=v7;
proc import datafile="&pg1dataset\nc-est2019-agesex-res.csv" 
            out=pg1.pop2
            dbms=csv
            replace;
    guessingrows=max;
run;


data pg1.pop_new;
    set pg1.pop2;
    length AgeClass $8 AgeGroup $6 Gender $7;
    
	Population=census2010pop; /* use 2010 population as reference population */
    retain gender sex age  Population AgeClass AgeGroup;

    /* Determine AgeClass based on age */
    if age = 999 then AgeClass = ' ';
    else if age >= 25 and age < 65 then AgeClass = 'age2564';
    else if age >= 65 then AgeClass = 'age65+';
    else AgeClass = ' '; /* Optional: Handles ages less than 25 or other undefined ages */

    /* Define new age group categories */
    if age = 999 then AgeGroup = ' ';
    else if age >= 25 and age < 35 then AgeGroup = '25-34';
    else if age >= 35 and age < 45 then AgeGroup = '35-44';
    else if age >= 45 and age < 55 then AgeGroup = '45-54';
    else if age >= 55 and age < 65 then AgeGroup = '55-64';
    else if age >= 65 and age < 75 then AgeGroup = '65-74';
    else if age >= 75 and age < 85 then AgeGroup = '75-84';
    else if age >= 85 then AgeGroup = '85+';
    else AgeGroup = ' '; /* Optional: Handles ages less than 25 or other undefined ages */
	
	/* Assuming that male is 1 female is 2 */
    if sex = 0 then gender = 'Total';
    else if sex = 1 then gender = 'Male';
    else if sex = 2 then gender = 'Female';
    else gender = 'Unknown'; 

	keep gender sex age  Population AgeClass AgeGroup;
run;

/* Aggregate population data */
proc sql;
    create table pg1.refer_pop2010 as
    select gender, AgeClass, AgeGroup, sum(Population) as Population_sum
    from pg1.pop_new
	where AgeGroup is not missing
    group by gender, ageclass, AgeGroup
	order by gender desc, ageclass, AgeGroup;
quit;


proc sql;
    create table pg1.pop_total as
    select AgeGroup, Population_sum
    from pg1.refer_pop2010
	where gender eq 'Total'
    order by AgeGroup;
quit;



proc sql;
    create table pg1.pop_sum_age2564 as
    select AgeGroup,Population_sum
    from pg1.refer_pop2010
	where AgeClass = 'age2564' and gender eq 'Total'
    order by AgeGroup;
quit;

proc sql;
    create table pg1.pop_sum_age65_ as
    select AgeGroup,sum(Population_sum) as Population_sum
    from pg1.refer_pop2010
	where AgeClass eq 'age65+' and gender in ('Female', 'Male')
    group by AgeGroup
	order by AgeGroup;
quit;

proc sql;
    create table pg1.pop_sum_f as
    select AgeGroup, Population_sum
    from pg1.refer_pop2010
	where gender eq 'Female' 
    order by AgeGroup;
quit;
proc sql;
    create table pg1.pop_sum_m as
    select AgeGroup, Population_sum
    from pg1.refer_pop2010
	where gender eq 'Male'
    order by AgeGroup;
quit;

proc sql;
    create table pg1.pop_sum_gen as
    select Gender,AgeGroup, Population_sum
    from pg1.refer_pop2010
	where gender in ('Male','Female')
    order by Gender,AgeGroup;
quit;



***********************************************************;
* Calculate age-adjusted standardization mortality rate   *;
***********************************************************;

%macro process_years(start_year=2010, end_year=2022);

/* Suppress all ODS output except for the final desired dataset */
ods exclude all;

/* Macro to process each year */
%do year = &start_year %to &end_year;
    %let pg1year_char = &year;

    /* SQL to sum data by demographics */
    proc sql;
        create table data_sum_&pg1year_char as
        select PopSize, Gender, AgeGroup, sum(deaths) as Deaths_sum, sum(population) as Population_sum
        from pg1.data_original2010_2022
        where year = "&pg1year_char"
        group by PopSize, Gender, AgeGroup
        order by PopSize, Gender, AgeGroup;
    quit;

    /* Standardized Rate Calculation */
    proc stdrate data=data_sum_&pg1year_char
                 refdata=pg1.pop_total
                 method=direct
                 stat=rate(mult=100000);
        population event=Deaths_sum total=Population_sum;
        reference total=Population_sum;
        by PopSize;
        strata AgeGroup/effect;
        ods output stdrate=total_rate_&pg1year_char(keep=Popsize StdRate LowerCL UpperCL);
    run;

    /* SQL for specific age group */
    proc sql;
        create table data_sum_age2564_&pg1year_char as
        select PopSize, AgeGroup, sum(deaths) as Deaths_sum, sum(population) as Population_sum
        from pg1.data_original2010_2022
        where year = "&pg1year_char" and ageclass = "age2564"
        group by PopSize, AgeGroup
        order by PopSize, AgeGroup;
    quit;

    proc stdrate data=data_sum_age2564_&pg1year_char
                 refdata=pg1.pop_sum_age2564
                 method=direct
                 stat=rate(mult=100000);
        population event=Deaths_sum total=Population_sum;
        reference total=Population_sum;
        by PopSize;
        strata AgeGroup/effect;
        ods output stdrate=out_age2564_rate_&pg1year_char(keep=Popsize StdRate LowerCL UpperCL);
    run;

    proc sql;
        create table data_sum_age65_&pg1year_char as
        select PopSize, AgeGroup, sum(deaths) as Deaths_sum, sum(population) as Population_sum
        from pg1.data_original2010_2022
        where year = "&pg1year_char" and ageclass = "age65+"
        group by PopSize, AgeGroup
        order by PopSize, AgeGroup;
    quit;

    proc stdrate data=data_sum_age65_&pg1year_char
                 refdata=pg1.pop_sum_age65_
                 method=direct
                 stat=rate(mult=100000);
        population event=Deaths_sum total=Population_sum;
        reference total=Population_sum;
        by PopSize;
        strata AgeGroup/effect;
        ods output stdrate=out_age65_rate_&pg1year_char(keep=Popsize StdRate LowerCL UpperCL);
    run;

    proc stdrate data=data_sum_&pg1year_char
                 refdata=pg1.pop_sum_gen
                 method=direct
                 stat=rate(mult=100000);
        population group=Gender event=Deaths_sum total=Population_sum;
        reference total=Population_sum;
        by PopSize;
        strata AgeGroup/effect;
        ods output stdrate=out_gen_rate_&pg1year_char(keep=Popsize Gender StdRate LowerCL UpperCL);
    run;

    /* Combine and format the rates */
    data Combined_Rates_&pg1year_char;
        set total_rate_&pg1year_char (in=a) out_age2564_rate_&pg1year_char (in=b) 
            out_age65_rate_&pg1year_char (in=c) out_gen_rate_&pg1year_char (in=d);
		length Rate_Format $50. Category $10.;

		if a then Category = 'overall';
        else if b then Category = 'age2564';
        else if c then Category = 'age65_';
        else if d then do;
            if Gender = 'Male' then Category = 'Male';
            else if Gender = 'Female' then Category = 'Female';
        end;
        format StdRate 10.2 LowerCL 10.2 UpperCL 10.2;
        
        Rate_Format = catx(" ", put(StdRate, 10.2), "(", put(LowerCL, 10.2), ",", put(UpperCL, 10.2), ")");
        keep PopSize Category Rate_Format;
    run;

    /* Sort and transpose */
    proc sort data=Combined_Rates_&pg1year_char;
        by PopSize descending Category;
    run;

    proc transpose data=Combined_Rates_&pg1year_char out=Combined_transposed_&pg1year_char;
        by PopSize;
        id Category;
        var Rate_Format;
    run;

    data Combined_transposed_&pg1year_char;
        set Combined_transposed_&pg1year_char;  
        Year = "&pg1year_char";
        drop _NAME_;
    run;

%end;

/* Re-enable ODS output to show only the combined dataset */
ods exclude none;

/* Combine all years into one dataset */
data Combined_long;
    set %do year = &start_year %to &end_year;
        Combined_transposed_&year 
    %end;
;
run;

/* Print the final combined dataset */
proc print data=Combined_long;
run;

ods exclude all; /* Optionally, turn off output again if needed */

%mend process_years;

/* Run the macro for the specified years */
%process_years(start_year=2010, end_year=2022);


ods exclude none;

proc sort data=Combined_long;
    by PopSize Year;
run;

/* Transpose the dataset */
proc transpose data=Combined_long out=trans_data;
    by PopSize ;
    id Year;
	var overall age65_ age2564 Male Female; 
run;

data pg1.final_data;
	retain PopSize  Category _2010 - _2022;
	length Category $10.; 
	set trans_data;
	Category =_name_;
	drop _name_;
	if Category = 'age65_' then Category = 'age65+';
run;

proc sort data=pg1.final_data;
   by PopSize descending Category;
run;


***********************************************************;
* Risk differences and P-value calculate *;
***********************************************************;

proc sql;
    create table pg1.data_2010vs2022 as
    select * from pg1.data_original2010_2022
    where year in ('2010', '2022');
quit;


/* SQL to sum data by demographics */
proc sql;
    create table data_risk_1 as
    select PopSize,  AgeGroup, Year, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from pg1.data_2010vs2022
	
    group by PopSize,  AgeGroup,Year
    order by PopSize,  AgeGroup,Year;
quit;

/* Standardized Rate Calculation */
proc stdrate data=data_risk_1
             refdata=pg1.pop_total
             method=direct
             stat=rate(mult=100000)
			 effect=diff;
    population group=year event=Deaths_sum total=Population_sum;
    reference total=Population_sum;
    by PopSize;
    strata AgeGroup/effect;
    ods output effect=total_risk_rate(keep=Popsize RateDiff LowerCL UpperCL ProbZ);
run;


/* SQL for specific age group */
proc sql;
    create table data_risk_age2564_1 as
    select PopSize, AgeGroup, Year, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from pg1.data_2010vs2022
    where ageclass = "age2564"
    group by PopSize, AgeGroup,Year
    order by PopSize, AgeGroup,Year;
quit;

proc stdrate data=data_risk_age2564_1
             refdata=pg1.pop_sum_age2564
             method=direct
             stat=rate(mult=100000)
			 effect=diff;
    population group=year event=Deaths_sum total=Population_sum;
    reference total=Population_sum;
    by PopSize;
    strata AgeGroup/effect;
    ods output effect=out_age2564_risk_rate(keep=Popsize RateDiff LowerCL UpperCL ProbZ);
run;

proc sql;
    create table data_risk_age65_1 as
    select PopSize, AgeGroup, Year, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from pg1.data_2010vs2022
    where ageclass = "age65+"
    group by PopSize, AgeGroup,Year
    order by PopSize, AgeGroup,Year;
quit;

proc stdrate data=data_risk_age65_1
             refdata=pg1.pop_sum_age65_
             method=direct
             stat=rate(mult=100000)
	         effect=diff;
    population group=year event=Deaths_sum total=Population_sum;
    reference total=Population_sum;
    by PopSize;
    strata AgeGroup/effect;
    ods output effect=out_age65_risk_rate(keep=Popsize RateDiff LowerCL UpperCL ProbZ);
run;

proc sql;
    create table data_risk_f_1 as
    select PopSize, AgeGroup, Year, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from pg1.data_2010vs2022
    where gender = "Female"
    group by PopSize, Year, AgeGroup
    order by PopSize, Year, AgeGroup;
quit;

proc stdrate data=data_risk_f_1
             refdata=pg1.pop_sum_gen
             method=direct
             stat=rate(mult=100000)
			 effect=diff;
    population group=Year event=Deaths_sum total=Population_sum;
    reference total=Population_sum;
    by PopSize;
    strata AgeGroup/effect;
    ods output effect=out_f_risk_rate(keep=Popsize RateDiff LowerCL UpperCL ProbZ);
run;

proc sql;
    create table data_risk_m_1 as
    select PopSize, AgeGroup, Year, sum(deaths) as Deaths_sum, sum(population) as Population_sum
    from pg1.data_2010vs2022
    where gender = "Male"
    group by PopSize, Year, AgeGroup
    order by PopSize, Year, AgeGroup;
quit;

proc stdrate data=data_risk_m_1
             refdata=pg1.pop_sum_gen
             method=direct
             stat=rate(mult=100000)
			 effect=diff;
    population group=Year event=Deaths_sum total=Population_sum;
    reference total=Population_sum;
    by PopSize;
    strata AgeGroup/effect;
    ods output effect=out_m_risk_rate(keep=Popsize RateDiff LowerCL UpperCL ProbZ);
run;

/* Combine and format the rates */
data pg1.Combined_Risk_Rates_1;
	retain PopSize Category Risk_Differences ProbZ;
    set total_risk_rate (in=a) out_age2564_risk_rate (in=b) 
        out_age65_risk_rate (in=c) out_f_risk_rate (in=d) 
        out_m_risk_rate(in=e);
	length Risk_Differences $50. Category $10.;

	if a then Category = 'overall';
    else if b then Category = 'age2564';
    else if c then Category = 'age65+';
    else if d then Category = 'Female';
    else if e then Category = 'Male';

    format RateDiff 10.2 LowerCL 10.2 UpperCL 10.2;
    
    Risk_Differences = catx(" ", put(RateDiff, 10.2), "(", put(LowerCL, 10.2), ",", put(UpperCL, 10.2), ")");
    keep PopSize Category Risk_Differences ProbZ;
run;

proc sort data=pg1.Combined_Risk_Rates_1;
   	by PopSize descending Category;
run;

*************** out put data ***************;
proc export data=pg1.final_data
    outfile="&pg1output\final_mortality_data.xlsx"
    dbms=xlsx
    replace;
run;

proc export data=pg1.Combined_Risk_Rates_1
    outfile="&pg1output\Risk_Differences.xlsx"
    dbms=xlsx
    replace;
run;


data pg1.output_data;
    merge pg1.final_data(in=a) pg1.combined_risk_rates_1(in=b);
    by PopSize descending Category ; 
    if a and b; 
run;

data pg1.output_data;
	retain PopSize Category _2010 - _2022 Empty_Column Risk_Differences ProbZ; 

    set pg1.output_data;
    length Empty_Column $1.; /* Defines an empty column with missing character value */
    /* Place the new empty column to divide two tables */
    keep PopSize Category _2010 - _2022 Empty_Column Risk_Differences ProbZ; 
run;

proc export data=pg1.output_data
    outfile="&pg1output\output_data.xlsx"
    dbms=xlsx
    replace;
run;

* Export data to Excel;
proc export data=pg1.output_data
    outfile="&pg1output\output_data.xlsx"  /* Corrected file path */
    dbms=xlsx
    replace;
run;

* Adding Titles and Footnotes left justification;
title1 j=l "Rate differences in age-adjusted cardiovascular mortality by rurality, 2010 -- 2022";
title2 j=r "Risk Differences 2010 - 2022";
footnote1 j=l "Source: CDC Wonder";
footnote2 j=l "* per 100,000 population";

* Define ODS Excel to output with specific Excel features;
ods excel file="&pg1output.\formatted_out_data.xlsx"
    options(sheet_name="Mortality Summary" embedded_titles="yes" embedded_footnotes="yes");

* Example PROC PRINT or other reporting procedure to generate output;
proc print data=pg1.output_data; 
run;

ods excel close;
title;
footnote;



