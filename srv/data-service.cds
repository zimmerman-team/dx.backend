using { data as my } from '../db/schema';
service CatalogService @(path:'/data') {

  @readonly entity IATIBudget as SELECT from my.IATIBudget {*} excluding { createdBy, modifiedBy };
}