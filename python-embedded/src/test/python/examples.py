# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import with_statement

import unit_tests
import tempfile, os

class ExamplesTest(unit_tests.GraphDatabaseTest):
    
    def test_hello_world(self):
        folder_to_put_db_in = tempfile.mkdtemp()
        try:
            # START SNIPPET: helloworld
            from neo4j import GraphDatabase
            
            # Create a database
            db = GraphDatabase(folder_to_put_db_in)
            
            # All write operations happen in a transaction
            with db.transaction:
                firstNode = db.node(name='Hello')
                secondNode = db.node(name='world!')
                
                # Create a relationship with type 'knows'
                relationship = firstNode.knows(secondNode, name='graphy')
                
            # Read operations can happen anywhere
            message = ' '.join([firstNode['name'], relationship['name'], secondNode['name']])
            
            print message
            
            # Delete the data
            with db.transaction:
                firstNode.knows.single.delete()
                firstNode.delete()
                secondNode.delete()
            
            # Always shut down your database when your application exits
            db.shutdown()
            # END SNIPPET: helloworld
        finally:
           if os.path.exists(folder_to_put_db_in):
              import shutil
              shutil.rmtree(folder_to_put_db_in)
              
        self.assertEqual(message, 'Hello graphy world!')
    
    def test_invoice_app(self):
        folder_to_put_db_in = tempfile.mkdtemp()
        try:
            # START SNIPPET: invoiceapp-setup
            from neo4j import GraphDatabase, INCOMING, Evaluation
            
            # Create a database
            db = GraphDatabase(folder_to_put_db_in)
            
            # All write operations happen in a transaction
            with db.transaction:
                
                # A node to connect customers to
                customers = db.node()
                
                # A node to connect invoices to
                invoices = db.node()
                
                # Connected to the reference node, so
                # that we can always find them.
                db.reference_node.CUSTOMERS(customers)
                db.reference_node.INVOICES(invoices)
                
                # An index, helps us rapidly look up customers
                customer_idx = db.node.indexes.create('customers')
            # END SNIPPET: invoiceapp-setup
            
            # START SNIPPET: invoiceapp-domainlogic-create
            def create_customer(name):
                with db.transaction:                    
                    customer = db.node(name=name)
                    customer.INSTANCE_OF(customers)
                    
                    # Index the customer by name
                    customer_idx['name'][name] = customer
                return customer
                
            def create_invoice(customer, amount):
                with db.transaction:
                    invoice = db.node(amount=amount)
                    invoice.INSTANCE_OF(invoices)
                    
                    invoice.SENT_TO(customer)
                return customer
            # END SNIPPET: invoiceapp-domainlogic-create
            
            # START SNIPPET: invoiceapp-domainlogic-get-by-idx
            def get_customer(name):
                return customer_idx['name'][name].single
            # END SNIPPET: invoiceapp-domainlogic-get-by-idx
            
            # START SNIPPET: invoiceapp-domainlogic-get-by-cypher
            def get_invoices_with_amount_over(customer, min_sum):       
                # Find all invoices over a given sum for a given customer.
                # Note that we return an iterator over the "invoice" column
                # in the result (['invoice']).
                return db.query('''START customer=node({customer_id})
                                   MATCH invoice-[:SENT_TO]->customer
                                   WHERE has(invoice.amount) and invoice.amount >= {min_sum}
                                   RETURN invoice''',
                                   customer_id = customer.id, min_sum = min_sum)['invoice']
            # END SNIPPET: invoiceapp-domainlogic-get-by-cypher
            
            # START SNIPPET: invoiceapp-create-and-search
            for name in ['Acme Inc.', 'Example Ltd.']:
               create_customer(name)
            
            # Loop through customers
            for relationship in customers.INSTANCE_OF:
               customer = relationship.start
               for i in range(1,12):
                   create_invoice(customer, 100 * i)
                   
            # Finding large invoices
            large_invoices = get_invoices_with_amount_over(get_customer('Acme Inc.'), 500)
            
            # Getting all invoices per customer:
            for relationship in get_customer('Acme Inc.').SENT_TO.incoming:
                invoice = relationship.start
            # END SNIPPET: invoiceapp-create-and-search
            
            self.assertEqual(len(list(large_invoices)), 7)
            db.shutdown()
        finally:
           if os.path.exists(folder_to_put_db_in):
              import shutil
              shutil.rmtree(folder_to_put_db_in)

if __name__ == '__main__':
    unit_tests.unittest.main()
