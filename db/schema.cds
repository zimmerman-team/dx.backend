using { managed } from '@sap/cds/common';
namespace data;

entity IATIBudget : managed {
  key ID : Integer;
  budget_period_start_iso_date  : localized String(1111);
  budget_period_end_iso_date  : localized String(1111);
  budget_value  : Decimal(32,2);
  budget_value_currency  : localized String(1111);
  budget_value_date  : localized String(1111);
  default_currency  : localized String(1111);
  default_lang  : localized String(1111);
  iati_identifier  : localized String(1111);
  reporting_org_ref  : localized String(1111);
  reporting_org_type  : localized String(1111);
  recipient_country_code  : localized String(1111);
  default_flow_type_code  : localized String(1111);
  default_aid_type_code  : localized String(1111);
  default_tied_status_code  : localized String(1111);
  budget_value_usd  : Decimal(32,2);
  budget_usd_conversion_rate  : Decimal(32,2);
}