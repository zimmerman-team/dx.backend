using { data as my } from '../db/schema';
service CatalogService @(path:'/data') {
	@readonly entity IATIAllBudgets as SELECT from my.IATIAllBudgets {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
	@readonly entity IATICovidActivities as SELECT from my.IATICovidActivities {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
	@readonly entity HXLPalestine as SELECT from my.HXLPalestine {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
	@readonly entity TGFPledgesContributions as SELECT from my.TGFPledgesContributions {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
}
