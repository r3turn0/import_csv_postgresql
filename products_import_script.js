//const fs = require('fs');
//const path = require('path');
const { Client } = require('pg');
//const csv = require('csv-parser');
require('dotenv').config({ path: '.env' });
const nodemailer = require('nodemailer');

// Constructing connection string to PostgreSql Database using environmental variables found in .env file
const connectionString = 'postgresql://' + process.env.DB_USER + ":" + process.env.DB_USER_PASSWORD + "@" + process.env.DB_HOST + ":" + process.env.DB_PORT + "/" + process.env.DB_NAME;
console.log('Connection string: ', connectionString);
//console.log('Directory path: ', directoryPath);

// Create a new PostgreSQL client
const client = new Client({
    connectionString: connectionString,
});

// Create a transporter object using SMTP transport
let transporter = nodemailer.createTransport({
    host: process.env.SMTP_SERVER,
    port: process.env.SMTP_SERVER_PORT,
    secure: true, // true for port 465, false for other ports
    service: process.env.SMTP_SERVER, // You can use other services like Yahoo, Outlook, etc.
    auth: {
        user: process.env.SMTP_USER, // Your email address
        pass: process.env.SMTP_PASSWORD   // Your email password
    }
});

let mailOptions = {
    from: '"' + process.env.UPLOADED_BY + '" ' + process.env.SMTP_USER, // Sender address
    to: process.env.SMTP_TO,          // List of recipients
    // subject: 'Hello from Node.js',              // Subject line
    // text: 'Hello world?',                       // Plain text body
    // html: '<b>Hello world?</b>'                 // HTML body
};

// Function to insert products using an INSERT SELECT statement in the table sample_tbl_1
async function insertProducts(callback) {
    await client.connect();
    console.log('Connection connected: ', client.host, client.database, client.port, client.user, client.password);
    const insertQuery = `INSERT INTO etc.products (filename, date_upload, uploaded_by,
                    external_id, item_id, display_name, item_name, item_number_name, 
                    vendor_name_code, sales_description, sales_packaging_unit, sale_qty_per_pack_unit, 
                    item_color, item_size, pcs_in_box, sqft_by_pcs_sheet, sqft_by_box, price_by_UOM, 
                    units_type, stock_unit, purchase_unit, sales_units, parent, subsidiary, 
                    include_children, department, "class", "location", costing_method, "cost", 
                    purchase_description, stock_description, match_bill_to_receipt, use_bins, 
                    supply_replenishment_method, alternate_demand_source_item, 
                    auto_preferred_stock_level, reorder_multiple, is_special_order_item, 
                    auto_reorder_point, auto_lead_time, lead_time, safety_stock_level, 
                    safety_stock_level_days, transfer_price, preferred_location, 
                    item_bin_number1, preferred_per_location, vendor1_name, vendor1_subsidiary, 
                    vendor1_preferred, vendor1_purchase_price, vendor1_schedule, vendor1_code, 
                    vendor2_name, vendor2_subsidiary, vendor2_preferred, 
                    vendor2_purchase_price, vendor2_code, item_location_line1_location, 
                    item_location_line1_default_return_cost, item_location_line1_preferred_stock_level, 
                    item_location_line1_reorder_print, item_location_line1_lot_numbers, 
                    item_location_line1_lot_sizing_numbers, cost_estimate_type, cost_estimate, 
                    minimum_quantity, enforce_qty_internally, item_price_line1_item_price_type_ref, 
                    item_price_line1_item_price, item_price_line1_quantity_pricing, cogs_account, income_account, asset_account, 
                    bill_price_variance_acct, bill_qty_variance_acct, bill_exch_variance_acct, 
                    cust_return_variance_account, vend_return_variance_account, tax_schedule, load_id) 
                    SELECT etc.sample_tbl_1.filename, etc.sample_tbl_1.date_upload::timestamp, etc.sample_tbl_1.uploaded_by,
                    etc.sample_tbl_1.external_id, etc.sample_tbl_1.item_id, etc.sample_tbl_1.display_name, etc.sample_tbl_1.item_name, etc.sample_tbl_1.item_number_name, 
                    etc.sample_tbl_1.vendor_name_code, etc.sample_tbl_1.sales_description, etc.sample_tbl_1.sales_packaging_unit, CASE WHEN etc.sample_tbl_1.sale_qty_per_pack_unit = '' THEN 0 ELSE etc.sample_tbl_1.sale_qty_per_pack_unit::float END, 
                    etc.sample_tbl_1.item_color, etc.sample_tbl_1.item_size, CASE WHEN etc.sample_tbl_1.pcs_in_box = '' THEN 0 ELSE etc.sample_tbl_1.pcs_in_box::numeric END, CASE WHEN etc.sample_tbl_1.sqft_by_pcs_sheet = '' THEN 0 ELSE etc.sample_tbl_1.sqft_by_pcs_sheet::float END, CASE WHEN etc.sample_tbl_1.sqft_by_box = '' THEN 0 ELSE etc.sample_tbl_1.sqft_by_box::float END, CASE WHEN etc.sample_tbl_1.price_by_UOM = '' THEN 0 ELSE etc.sample_tbl_1.price_by_UOM::float END, 
                    etc.sample_tbl_1.units_type, etc.sample_tbl_1.stock_unit, etc.sample_tbl_1.purchase_unit, etc.sample_tbl_1.sales_units, etc.sample_tbl_1.parent, etc.sample_tbl_1.subsidiary, 
                    etc.sample_tbl_1.include_children, etc.sample_tbl_1.department, CASE WHEN etc.sample_tbl_1.class = '' THEN 0 ELSE etc.sample_tbl_1.class::numeric END, etc.sample_tbl_1.location, etc.sample_tbl_1.costing_method, CASE WHEN etc.sample_tbl_1.cost = '' THEN 0 ELSE etc.sample_tbl_1.cost::float END, 
                    etc.sample_tbl_1.purchase_description, etc.sample_tbl_1.stock_description, etc.sample_tbl_1.match_bill_to_receipt, etc.sample_tbl_1.use_bins, 
                    etc.sample_tbl_1.supply_replenishment_method, etc.sample_tbl_1.alternate_demand_source_item, 
                    etc.sample_tbl_1.auto_preferred_stock_level, etc.sample_tbl_1.reorder_multiple, etc.sample_tbl_1.is_special_order_item, 
                    etc.sample_tbl_1.auto_reorder_point, etc.sample_tbl_1.auto_lead_time, etc.sample_tbl_1.lead_time, etc.sample_tbl_1.safety_stock_level, 
                    etc.sample_tbl_1.safety_stock_level_days, etc.sample_tbl_1.transfer_price, etc.sample_tbl_1.preferred_location, 
                    etc.sample_tbl_1.item_bin_number1, etc.sample_tbl_1.preferred_per_location, etc.sample_tbl_1.vendor1_name, CASE WHEN etc.sample_tbl_1.vendor1_subsidiary = '' THEN 0 ELSE etc.sample_tbl_1.vendor1_subsidiary::numeric END, 
                    etc.sample_tbl_1.vendor1_preferred, CASE WHEN etc.sample_tbl_1.vendor1_purchase_price = '' THEN 0 ELSE etc.sample_tbl_1.vendor1_purchase_price::float END, etc.sample_tbl_1.vendor1_schedule, etc.sample_tbl_1.vendor1_code, 
                    etc.sample_tbl_1.vendor2_name, CASE WHEN etc.sample_tbl_1.vendor2_subsidiary = '' THEN 0 ELSE etc.sample_tbl_1.vendor2_subsidiary::numeric END, etc.sample_tbl_1.vendor2_preferred, 
                    CASE WHEN etc.sample_tbl_1.vendor2_purchase_price = '' THEN 0 ELSE etc.sample_tbl_1.vendor2_purchase_price::float END, etc.sample_tbl_1.vendor2_code, etc.sample_tbl_1.item_location_line1_location, 
                    etc.sample_tbl_1.item_location_line1_default_return_cost, etc.sample_tbl_1.item_location_line1_preferred_stock_level, 
                    etc.sample_tbl_1.item_location_line1_reorder_print, etc.sample_tbl_1.item_location_line1_lot_numbers, 
                    CASE WHEN etc.sample_tbl_1.item_location_line1_lot_sizing_numbers = '' THEN 0 ELSE etc.sample_tbl_1.item_location_line1_lot_sizing_numbers::numeric END, etc.sample_tbl_1.cost_estimate_type, CASE WHEN etc.sample_tbl_1.cost_estimate = '' THEN 0 ELSE etc.sample_tbl_1.cost_estimate::float END, 
                    CASE WHEN etc.sample_tbl_1.minimum_quantity = '' THEN 0 ELSE etc.sample_tbl_1.minimum_quantity::numeric END, etc.sample_tbl_1.enforce_qty_internally, etc.sample_tbl_1.item_price_line1_item_price_type_ref, 
                    CASE WHEN etc.sample_tbl_1.item_price_line1_item_price = '' THEN 0 ELSE etc.sample_tbl_1.item_price_line1_item_price::float END, CASE WHEN etc.sample_tbl_1.item_price_line1_quantity_pricing = '' THEN 0 ELSE etc.sample_tbl_1.item_price_line1_quantity_pricing::numeric END, etc.sample_tbl_1.cogs_account, etc.sample_tbl_1.income_account, etc.sample_tbl_1.asset_account, 
                    etc.sample_tbl_1.bill_price_variance_acct, etc.sample_tbl_1.bill_qty_variance_acct, etc.sample_tbl_1.bill_exch_variance_acct, 
                    etc.sample_tbl_1.cust_return_variance_account, etc.sample_tbl_1.vend_return_variance_account, etc.sample_tbl_1.tax_schedule,
                    etc.sample_tbl_1.load_id FROM etc.sample_tbl_1`;
    // Get the most recent file upload in the etc.sample_tbl_1 table and check to see if the row count is > 0; Meaning there's a file waiting to be imported.
    const selectQuery = `SELECT filename, date_upload, uploaded_by, load_id FROM etc.sample_tbl_1 ORDER BY date_upload DESC LIMIT 1`;
    const selectQueryResult = await client.query(selectQuery);
    if(selectQueryResult.rowCount !== 0) {
        try {
            const subject = 'etc.products IMPORT on ' + selectQueryResult.rows[0].date_upload;
            let text = 'The file with load_id: ' + selectQueryResult.rows[0].load_id + ' and filename: ' + selectQueryResult.rows[0].filename + ' has been imported into the etc.products table by ' + selectQueryResult.rows[0].uploaded_by + ' on ' + selectQueryResult.rows[0].date_upload;
            const load_id = selectQueryResult.rows[0].load_id;
            const filename = selectQueryResult.rows[0].filename;
            const uploaded_by = selectQueryResult.rows[0].uploaded_by;
            const date_upload = selectQueryResult.rows[0].date_upload;
            console.log('Inserting products using query: ', insertQuery);
            const results = await client.query(insertQuery);
            const num_rows = results.rowCount;
            const html = '<p>' + 
                            text + 
                        '</p>' + 
                        '<p> Load Id: ' + 
                            load_id  + 
                        '</p>' + 
                        '<p> Filename: ' + 
                            filename  + 
                        '</p>' + 
                        '<p> Uploaded By: ' + 
                            uploaded_by  + 
                        '</p>' + 
                        '<p> Date Upload: ' + 
                            date_upload  + 
                        '</p>' + 
                        '<p> Number of Rows: ' + 
                            num_rows  + 
                        '</p>';
            let insertResultObj = {
                subject: subject,
                text: text,
                html: html           
            }
            console.log('Rows inserted: ', results.rowCount);
            if(results.rowCount > 0) {
                console.log('Truncating table etc.sample_tbl_1...');
                await client.query('TRUNCATE TABLE etc.sample_tbl_1 RESTART IDENTITY');
                const res = await client.query('SELECT COUNT(*) FROM etc.sample_tbl_1');
                const count = res.rowCount;
                if(count === 0 || !count) {
                    console.log('Table etc.sample_tbl_1 has been truncated. Row count: ', count);
                }
                else {
                    console.log('Table etc.sample_tbl_1 has not been truncated ...');
                }
            } 
            await client.end();
            callback(insertResultObj);
        }
        catch (e) {
            await client.end();
            console.error('Error inserting products: ', e);
            if(selectQueryResult.rowCount === 0) {
                console.error('No rows found in the etc.sample_tbl_1 table');
            }
            else {
                const subject = 'etc.products IMPORT on ' + selectQueryResult.rows[0].date_upload;
                let text = 'The file with load_id: ' + selectQueryResult.rows[0].load_id + ' and filename: ' + selectQueryResult.rows[0].filename + ' has not been imported into the etc.products table by ' + selectQueryResult.rows[0].uploaded_by + ' on ' + selectQueryResult.rows[0].date_upload;
                const load_id = selectQueryResult.rows[0].load_id;
                const filename = selectQueryResult.rows[0].filename;
                const uploaded_by = selectQueryResult.rows[0].uploaded_by;
                const date_upload = selectQueryResult.rows[0].date_upload;
                text += '\nWith Error: ' + e;
                const html = '<p>' + 
                                text + 
                            '</p>' + 
                            '<p> Load Id: ' + 
                                load_id  + 
                            '</p>' + 
                            '<p> Filename: ' + 
                                filename  + 
                            '</p>' + 
                            '<p> Uploaded By: ' + 
                                uploaded_by  + 
                            '</p>' + 
                            '<p> Date Upload: ' + 
                                date_upload  + 
                            '</p>';
                let errorObj = {
                    error: e,
                    subject: subject,
                    text: text,
                    html: html           
                }
                callback(errorObj);
            }
        }
    }
}

// Insert products into the database
insertProducts(function(res) {
    if(res.error) {
        console.error('There was an error in inserting into the products table: ', res.error); 
        mailOptions.subject = res.subject;
        mailOptions.text = res.text;
        mailOptions.html = res.html;
        transporter.sendMail(mailOptions, function(err, info) {
            if(err) {
                console.error('Error sending email: ', err);
            }
            else {
                console.log('Email sent: ', info.response);
            }
        });
    }
    else {
        if(res.num_rows > 0) {
            console.log('Products inserted successfully');
        }
        if (res.num_rows === 0) {
            console.log('No new products inserted');
        }
        mailOptions.subject = res.subject;
        mailOptions.text = res.text;
        mailOptions.html = res.html;
        transporter.sendMail(mailOptions, function(err, info) {
            if(err) {
                console.error('Error sending email: ', err);
            }
            else {
                console.log('Email sent: ', info.response);
            }
        });
    }
});