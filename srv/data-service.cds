using { data as my } from '../db/schema';
service CatalogService @(path:'/data') {
	@readonly entity IATIActivity as SELECT from my.IATIActivity {*} excluding { createdBy, modifiedBy };
	@readonly entity IATIAllBudgets as SELECT from my.IATIAllBudgets {*} excluding { createdBy, modifiedBy };
	@readonly entity IATIBudget as SELECT from my.IATIBudget {*} excluding { createdBy, modifiedBy };
	@readonly entity IATITransaction as SELECT from my.IATITransaction {*} excluding { createdBy, modifiedBy };
	@readonly entity TGFAllocation as SELECT from my.TGFAllocation {*} excluding { createdBy, modifiedBy };
	@readonly entity HXLPalestine as SELECT from my.HXLPalestine {*} excluding { createdBy, modifiedBy };
	@readonly entity HXLUkraineFunding as SELECT from my.HXLUkraineFunding {*} excluding { createdBy, modifiedBy };
}
