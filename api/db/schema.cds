using { managed } from '@sap/cds/common';
namespace data;

entity dx635a6f346692175e6f886613 : managed {
	key ID : Integer;
	DonorName  : String;
	Textbox5  : String;
	GeographicAreaCode_ISO2  : String;
	GeographicAreaCode_ISO3  : String;
	GeographicAreaName  : String;
	DonorParentName  : String;
	ReplenishmentCycle  : String;
	Indicator  : String;
	TransactionType  : String;
	SourceCurrency  : String;
	Year  : Decimal;
	Month  : String;
	Amount  : Decimal;
}

entity dx635baaea58a3994c29e74d3f : managed {
	key ID : Integer;
	date  : DateTime;
	budgetYear  : Decimal;
	description  : String;
	amountUSD  : Decimal;
	srcOrganization  : String;
	srcOrganizationTypes  : String;
	srcLocations  : String;
	srcUsageYearStart  : Decimal;
	srcUsageYearEnd  : Decimal;
	destPlan  : String;
	destPlanCode  : String;
	destPlanId  : Decimal;
	destOrganization  : String;
	destOrganizationTypes  : String;
	destGlobalClusters  : String;
	destLocations  : String;
	destProject  : String;
	destProjectCode  : String;
	destEmergency  : String;
	destUsageYearStart  : Decimal;
	destUsageYearEnd  : Decimal;
	contributionType  : String;
	flowType  : String;
	method  : String;
	boundary  : String;
	onBoundary  : String;
	status  : String;
	firstReportedDate  : DateTime;
	decisionDate  : DateTime;
	keywords  : String;
	originalAmount  : Decimal;
	originalCurrency  : String;
	exchangeRate  : Decimal;
	datasource_id  : Decimal;
	refCode  : String;
	createdAt  : DateTime;
	updatedAt  : DateTime;
}
