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

// Function to insert products using an INSERT SELECT statement in the table staging
async function insertProducts(callback) {
    await client.connect();
    console.log('Connection connected: ', client.host, client.database, client.port, client.user, client.password);
    const insertQuery = `INSERT INTO etc.product (filename, date_upload, uploaded_by,
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
                    SELECT etc.staging.filename, etc.staging.date_upload::timestamp, etc.staging.uploaded_by,
                    etc.staging.external_id, etc.staging.item_id, etc.staging.display_name, etc.staging.item_name, etc.staging.item_number_name, 
                    etc.staging.vendor_name_code, etc.staging.sales_description, etc.staging.sales_packaging_unit, CASE WHEN etc.staging.sale_qty_per_pack_unit = '' THEN 0 ELSE etc.staging.sale_qty_per_pack_unit::float END, 
                    etc.staging.item_color, etc.staging.item_size, CASE WHEN etc.staging.pcs_in_box = '' THEN 0 ELSE etc.staging.pcs_in_box::numeric END, CASE WHEN etc.staging.sqft_by_pcs_sheet = '' THEN 0 ELSE etc.staging.sqft_by_pcs_sheet::float END, CASE WHEN etc.staging.sqft_by_box = '' THEN 0 ELSE etc.staging.sqft_by_box::float END, CASE WHEN etc.staging.price_by_UOM = '' THEN 0 ELSE etc.staging.price_by_UOM::float END, 
                    etc.staging.units_type, etc.staging.stock_unit, etc.staging.purchase_unit, etc.staging.sales_units, etc.staging.parent, etc.staging.subsidiary, 
                    etc.staging.include_children, etc.staging.department, CASE WHEN etc.staging.class = '' THEN 0 ELSE etc.staging.class::numeric END, etc.staging.location, etc.staging.costing_method, CASE WHEN etc.staging.cost = '' THEN 0 ELSE etc.staging.cost::float END, 
                    etc.staging.purchase_description, etc.staging.stock_description, etc.staging.match_bill_to_receipt, etc.staging.use_bins, 
                    etc.staging.supply_replenishment_method, etc.staging.alternate_demand_source_item, 
                    etc.staging.auto_preferred_stock_level, etc.staging.reorder_multiple, etc.staging.is_special_order_item, 
                    etc.staging.auto_reorder_point, etc.staging.auto_lead_time, etc.staging.lead_time, etc.staging.safety_stock_level, 
                    etc.staging.safety_stock_level_days, etc.staging.transfer_price, etc.staging.preferred_location, 
                    etc.staging.item_bin_number1, etc.staging.preferred_per_location, etc.staging.vendor1_name, CASE WHEN etc.staging.vendor1_subsidiary = '' THEN 0 ELSE etc.staging.vendor1_subsidiary::numeric END, 
                    etc.staging.vendor1_preferred, CASE WHEN etc.staging.vendor1_purchase_price = '' THEN 0 ELSE etc.staging.vendor1_purchase_price::float END, etc.staging.vendor1_schedule, etc.staging.vendor1_code, 
                    etc.staging.vendor2_name, CASE WHEN etc.staging.vendor2_subsidiary = '' THEN 0 ELSE etc.staging.vendor2_subsidiary::numeric END, etc.staging.vendor2_preferred, 
                    CASE WHEN etc.staging.vendor2_purchase_price = '' THEN 0 ELSE etc.staging.vendor2_purchase_price::float END, etc.staging.vendor2_code, etc.staging.item_location_line1_location, 
                    etc.staging.item_location_line1_default_return_cost, etc.staging.item_location_line1_preferred_stock_level, 
                    etc.staging.item_location_line1_reorder_print, etc.staging.item_location_line1_lot_numbers, 
                    CASE WHEN etc.staging.item_location_line1_lot_sizing_numbers = '' THEN 0 ELSE etc.staging.item_location_line1_lot_sizing_numbers::numeric END, etc.staging.cost_estimate_type, CASE WHEN etc.staging.cost_estimate = '' THEN 0 ELSE etc.staging.cost_estimate::float END, 
                    CASE WHEN etc.staging.minimum_quantity = '' THEN 0 ELSE etc.staging.minimum_quantity::numeric END, etc.staging.enforce_qty_internally, etc.staging.item_price_line1_item_price_type_ref, 
                    CASE WHEN etc.staging.item_price_line1_item_price = '' THEN 0 ELSE etc.staging.item_price_line1_item_price::float END, CASE WHEN etc.staging.item_price_line1_quantity_pricing = '' THEN 0 ELSE etc.staging.item_price_line1_quantity_pricing::numeric END, etc.staging.cogs_account, etc.staging.income_account, etc.staging.asset_account, 
                    etc.staging.bill_price_variance_acct, etc.staging.bill_qty_variance_acct, etc.staging.bill_exch_variance_acct, 
                    etc.staging.cust_return_variance_account, etc.staging.vend_return_variance_account, etc.staging.tax_schedule,
                    etc.staging.load_id FROM etc.staging`;
    // Get the most recent file upload in the etc.staging table and check to see if the row count is > 0; Meaning there's a file waiting to be imported.
    const selectQuery = `SELECT filename, date_upload, uploaded_by, load_id FROM etc.staging ORDER BY date_upload DESC LIMIT 1`;
    const selectQueryResult = await client.query(selectQuery);
    const load_id = selectQueryResult.rows[0].load_id;
    const filename = selectQueryResult.rows[0].filename;
    const uploaded_by = selectQueryResult.rows[0].uploaded_by;
    const date_upload = selectQueryResult.rows[0].date_upload;
    const subject = 'IMPORT to etc.product: ' + filename + ' on ' + date_upload;
    let text = 'The file with load_id: ' + load_id + ' and filename: ' + filename + ' has been imported into the etc.product table by ' + uploaded_by + ' on ' + date_upload;
    let html = '';
    if(selectQueryResult.rowCount !== 0) {
        try {
            console.log('Inserting products using query: ', insertQuery);
            const results = await client.query(insertQuery);
            const num_rows = results.rowCount;
            html = '<h1>Product Import</h1><p>' + 
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
                html: html,
                num_rows: num_rows           
            }
            console.log('Rows inserted: ', results.rowCount);
            if(num_rows > 0) {
                console.log('Truncating table etc.staging...');
                await client.query('TRUNCATE TABLE etc.staging RESTART IDENTITY');
                console.log('Table etc.staging has been truncated');
            } 
            await client.end();
            callback(insertResultObj);
        }
        catch (e) {
            await client.end();
            console.error('Error inserting products: ', e);
            if(selectQueryResult.rowCount === 0) {
                console.error('No rows found in the etc.staging table');
            }
            else {
                text = 'The file with load_id: ' + load_id + ' and filename: ' + filename + ' has not been imported into the etc.product table by ' + uploaded_by + ' on ' + date_upload + '  with Error: ' + e;
                html = '<p>' + 
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
    mailOptions.subject = res.subject;
    mailOptions.text = res.text;
    mailOptions.html = res.html;
    if(res.error) {
        console.error('There was an error in inserting into the products table: ', res.error); 
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