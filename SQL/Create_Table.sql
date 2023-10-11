CREATE TABLE IF NOT EXISTS listing_companies (
 	ticker varchar(8) unique,
 	exchange varchar(5),
 	short_name varchar(256),
 	industry_id double precision,
 	industry_idv2 integer,
 	industry varchar(256),
 	industry_en varchar(256),
 	established_year integer,
 	company_type varchar(2),
 	PRIMARY KEY (ticker)
 );
 
 CREATE TABLE IF NOT EXISTS companies_info (
 	ticker varchar(8) unique,
 	no_employees integer,
 	no_shareholders integer,
 	foreign_percent double precision,
 	website varchar(256),
 	stock_rating double precision,
 	delta_in_week double precision,
 	delta_in_month double precision,
 	delta_in_year double precision,
 	outstanding_share double precision,
 	issue_share double precision,
 	PRIMARY KEY (ticker),
	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
CREATE TABLE IF NOT EXISTS stock_history (
 	ticker varchar(3),
 	time_stamp timestamp not null,
 	open double precision,
 	high double precision,
 	low double precision,
 	close double precision,
 	volume integer,
 	PRIMARY KEY (ticker, time_stamp),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
CREATE TABLE IF NOT EXISTS derived_stock_history (
 	ticker varchar(3),
 	time_stamp timestamp not null,
 	sma	 double precision,
 	lower_bollinger double precision,
 	higher_bollinger double precision,
	money_flow double precision,
 	PRIMARY KEY (ticker, time_stamp),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
CREATE TABLE IF NOT EXISTS income_statement (
 	ticker varchar(3),
 	year integer,
 	quarter integer,
 	revenue integer,
 	year_revenue_growth double precision,
 	quarter_revenue_growth double precision,
 	cost_of_good_sold double precision,
 	gross_profit double precision,
 	operation_expense integer,
 	operation_profit integer,
 	year_operation_profit_growth double precision,
 	quarter_operation_profit_growth double precision,
 	interest_expense double precision,
 	pre_tax_profit integer,
 	post_tax_profit integer,
 	share_holder_income integer,
 	year_share_holder_income_growth double precision,
 	quarter_share_holder_income_growth double precision,
 	invest_profit double precision,
 	service_profit double precision,
 	other_profit double precision,
 	provision_expense double precision,
 	operation_income double precision,
 	ebitda double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS balance_sheet (
 	ticker varchar(3),
 	year integer,
 	quarter integer,
 	short_asset double precision,
 	cash integer,
 	short_invest double precision,
 	short_receivable double precision,
 	inventory double precision,
 	long_asset double precision,
 	fixed_asset integer,
 	asset integer,
 	debt integer,
 	short_debt double precision,
 	long_debt double precision,
 	equity integer,
 	capital integer,
 	central_bank_deposit double precision,
 	other_bank_deposit double precision,
 	other_bank_loan double precision,
 	stock_invest double precision,
 	customer_loan double precision,
 	bad_loan double precision,
 	provision double precision,
 	net_customer_loan double precision,
 	other_asset double precision,
 	other_bank_credit double precision,
 	owe_other_bank double precision,
 	owe_central_bank double precision,
 	valuable_paper double precision,
 	payable_interest double precision,
 	receivable_interest double precision,
 	deposit double precision,
 	other_debt integer,
 	fund double precision,
 	un_distributed_income double precision,
 	minor_share_holder_profit double precision,
 	payable integer,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS cash_flow (
 	ticker varchar(3),
 	year integer,
 	quarter integer,
 	invest_cost integer,
 	from_invest integer,
 	from_financial integer,
 	from_sale integer,
 	free_cash_flow double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS financial_ratio (
 	ticker varchar(3),
 	year integer,
 	quarter integer,
 	price_to_earning double precision,
 	price_to_book double precision,
 	value_before_ebitda double precision,
 	dividend double precision,
 	roe double precision,
 	roa double precision,
 	days_receivable double precision,
 	days_inventory double precision,
 	days_payable double precision,
 	ebit_on_interest double precision,
 	earning_per_share integer,
 	book_value_per_share double precision,
 	interest_margin double precision,
 	non_interest_on_toi double precision,
 	bad_debt_percentage double precision,
 	provision_on_bad_debt double precision,
 	cost_of_financing double precision,
 	equity_on_total_asset double precision,
 	equity_on_loan double precision,
 	cost_to_income double precision,
 	equity_on_liability double precision,
 	current_payment double precision,
 	quick_payment double precision,
 	eps_change double precision,
 	ebitda_on_stock double precision,
 	gross_profit_margin double precision,
 	operating_profit_margin double precision,
 	post_tax_margin double precision,
 	debt_on_equity double precision,
 	debt_on_asset double precision,
 	debt_on_ebitda double precision,
 	short_on_long_debt double precision,
 	asset_on_equity double precision,
 	capital_balance double precision,
 	cash_on_equity double precision,
 	cash_on_capitalize double precision,
 	cash_circulation double precision,
 	revenue_on_work_capital double precision,
 	capex_on_fixed_asset double precision,
 	revenue_on_asset double precision,
 	post_tax_on_pre_tax double precision,
 	ebit_on_revenue double precision,
 	pre_tax_on_ebit double precision,
 	pre_provision_on_toi double precision,
 	post_tax_on_toi double precision,
 	loan_on_earn_asset double precision,
 	loan_on_asset double precision,
 	loan_on_deposit double precision,
 	deposit_on_earn_asset double precision,
 	bad_debt_on_asset double precision,
 	liquidity_on_liability double precision,
 	payable_on_equity double precision,
 	cancel_debt double precision,
 	ebitda_on_stock_change double precision,
 	book_value_per_share_change double precision,
 	credit_growth double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS stock_intraday_transaction (
 	id serial,
 	price double precision,
 	volume integer,
 	cp double precision,
 	rcp double precision,
 	a varchar(2),
 	ba double precision,
 	sa double precision,
 	hl boolean,
 	pcp double precision,
 	time_stamp timestamp not null,
 	ticker varchar(3),
 	primary key (id, time_stamp),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS general_rating (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	stock_rating double precision,
 	valuation double precision,
 	financial_health double precision,
 	business_model double precision,
 	business_operation double precision,
 	rs_rating double precision,
 	ta_score double precision,
 	highest_price double precision,
 	lowest_price double precision,
 	price_change3m double precision,
 	price_change1y double precision,
 	beta double precision,
 	alpha double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS business_model_rating (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	business_model double precision,
 	business_efficiency double precision,
 	asset_quality double precision,
 	cash_flow_quality double precision,
 	bom double precision,
 	business_administration double precision,
 	product_service double precision,
 	business_advantage double precision,
 	company_position double precision,
 	industry double precision,
 	operation_risk double precision,
 	PRIMARY KEY (ticker),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS business_operation_rating (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	industry_en varchar(256),
 	loan_growth double precision,
 	deposit_growth double precision,
 	net_interest_income_growth double precision,
 	net_interest_margin double precision,
 	cost_to_income double precision,
 	net_income_to_i double precision,
 	business_operation double precision,
 	avg_ro_e double precision,
 	avg_ro_a double precision,
 	last5years_net_profit_growth double precision,
 	last5years_revenue_growth double precision,
 	last5years_operating_profit_growth double precision,
 	last5years_eb_it_da_growth double precision,
 	last5years_fc_ff_growth double precision,
 	last_year_gross_profit_margin double precision,
 	last_year_operating_profit_margin double precision,
 	last_year_net_profit_margin double precision,
 	t_oi_growth double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS financial_health_rating (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	industry_en varchar(256),
 	loan_deposit double precision,
 	bad_loan_gross_loan double precision,
 	bad_loan_asset double precision,
 	provision_bad_loan double precision,
 	financial_health double precision,
 	net_debt_equity double precision,
 	current_ratio double precision,
 	quick_ratio double precision,
 	interest_coverage double precision,
 	net_debt_eb_it_da double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS valuation_rating (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	industry_en varchar(256),
 	valuation double precision,
 	pe double precision,
 	pb double precision,
 	ps double precision,
 	evebitda double precision,
 	dividend_rate double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
CREATE TABLE IF NOT EXISTS industry_financial_health (
 	ticker varchar(8),
	year integer,
 	quarter integer,
 	industry_en double precision,
 	loan_deposit double precision,
 	bad_loan_gross_loan double precision,
 	bad_loan_asset double precision,
 	provision_bad_loan double precision,
 	financial_health double precision,
 	net_debt_equity double precision,
 	current_ratio double precision,
 	quick_ratio double precision,
 	interest_coverage double precision,
 	net_debt_eb_it_da double precision,
 	PRIMARY KEY (ticker, year, quarter),
 	FOREIGN KEY (ticker) REFERENCES listing_companies (ticker) ON DELETE CASCADE
 );
 
 
