using { data as my } from '../db/schema';
service CatalogService @(path:'/data') {
	@readonly entity dx635a6f346692175e6f886613 as SELECT from my.dx635a6f346692175e6f886613 {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
	@readonly entity dx635baaea58a3994c29e74d3f as SELECT from my.dx635baaea58a3994c29e74d3f {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };
}
