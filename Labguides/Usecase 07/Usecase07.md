<!--
lab:
  title: 'Use Case 07: Next-gen analytics with Fabric IQ, Data Agents and Project Rayfin'
  description: In this exercise, you have created a notebook and trained a machine learning model. You used Scikit-Learn to train the model and MLflow to track it´s performance.
  duration: 5 minutes
  level: 200
  islab: true
  primarytopics:
    - Microsoft Fabric
-->

# Lab 7: Next-gen analytics with Fabric IQ, Data Agents and Project Rayfin

**Scenario**

**Lakeshore Retail** is a fictional company that sells ice cream at
multiple store locations. Sales data, product details and store
information are stored in a lakehouse, while freezer sensors stream
temperature and humidity readings into an Eventhouse. Today, business
users have to know which tables to join and which systems to query to
answer simple cross-domain questions, such as *"Which stores have lower
ice cream sales when freezer temperature rises above -18 °C?"*

Lakeshore Retail wants a **business-centric semantic layer** that
describes its business in its own terms — stores, products, sale events
and freezers — and connects those concepts to the underlying data. With
this layer, analysts can explore relationships visually and ask
questions in natural language, without needing to understand the
underlying tables or schemas.

As a data engineer, you will prepare a Fabric workspace, load sales and
telemetry data, build an ontology with **Fabric IQ Ontology (preview)**,
explore it with graph queries, connect it to a **Fabric data agent** for
natural language questions, and build and deploy a companion app with
**Project Rayfin**.

**Introduction**

In modern data platforms, enterprises often need a **business-centric
semantic layer** that unifies meaning across diverse data sources and
analytical models. The **Ontology (preview)** feature in Microsoft
Fabric IQ enables you to build this layer by defining **enterprise
concepts** (like products, stores, and events) and **their
relationships**, and then binding these definitions to real data across
your lakehouse, semantic models, and event streams.

In this lab, you use sample data to build an ontology that captures
business concepts such as *Store*, *Products*, *SaleEvent* and
*Freezer*. You also connect streaming data (freezer telemetry from
Eventhouse) to these concepts so the ontology can support
**cross-domain reasoning and queries**.

**Fabric items created in this lab**

| **Item** | **Name** | **Role in the lab** |
|----|----|----|
| Workspace | Fabric IQ Ontology\<lab instance ID\> | Contains all the items for the lab |
| Lakehouse | IQ_Lakehouse | Stores the product, store, sales and freezer tables |
| Eventhouse / KQL database | TelemetryDataEH | Stores the FreezerTelemetry time-series data |
| Ontology (preview) | RetailSalesOntology | Defines the Store, Products, SaleEvent and Freezer entity types and their relationships |
| Data agent (preview) | RetailOntologyAgent | Answers natural language questions grounded in the ontology |
| Fabric data app + SQL Database | Fabricapp | Companion Todo app built and deployed with Project Rayfin |

**Objectives**:

- Prepare a Microsoft Fabric workspace with the required items,
  including Lakehouse, Eventhouse, and Ontology (preview).

- Build a business-centric ontology by defining core entity types such
  as Store, Products, SaleEvent, and Freezer.

- Bind static data from OneLake tables and time-series data from
  Eventhouse to ontology entities.

- Create relationships between entities that represent real business
  processes (for example, SaleEvent from Store and Store operates
  Freezer).

- Explore and validate the ontology using entity instances,
  relationship graphs, and query builder filters.

- Enable natural language querying by integrating the ontology with a
  Fabric data agent (preview).

- Build, test and deploy a companion app to Fabric with Project Rayfin.

## Exercise 1: Environment setup

In this exercise, you create a Fabric workspace, load the sample sales
data into a lakehouse, and upload the freezer telemetry data to an
Eventhouse.

### Task 1: Create a Fabric workspace

In this task, you create a Fabric workspace. The workspace contains all
the items needed for this lab, including the lakehouse, eventhouse,
ontology, and data agent.

1.  Open your browser, navigate to the address bar, and type or paste
    the following URL: +++https://app.fabric.microsoft.com/+++ then press
    the **Enter** button and sign in with the following credentials.

| **Username** | **+++@lab.CloudPortalCredential(User1).Username+++** |
|----|----|
| **Password** | **+++@lab.CloudPortalCredential(User1).Password+++** |

2.  In the portal, switch to **Fabric** mode before proceeding to create
    the workspace.

![](./media/image1.png)

3.  In the **Workspaces** pane, click on the **+ New workspace** tile.

![](./media/image2.png)

4.  In the **Create a workspace** pane that appears on the right side,
    enter the following details, and click on the **Apply** button.

| **Setting** | **Value** |
|----|----|
| **Name** | +++Fabric IQ Ontology@lab.LabInstance.Id+++ |
| **Advanced** | Under **License mode**, select **Fabric capacity** |
| **Default storage format** | **Small dataset storage format** |

![](./media/image3.png)

![](./media/image4.png)

![](./media/image5.png)

### Task 2: Create a lakehouse

1.  Create a new lakehouse by clicking on the **+ New item** button in
    the navigation bar.

![](./media/image6.png)

2.  Filter by +++Lakehouse+++ and select the **Lakehouse** tile.

![](./media/image7.png)

3.  In the **New lakehouse** dialog box, enter +++IQ_Lakehouse+++ in the
    **Name** field and **unselect** **Lakehouse schemas**. Click on the
    **Create** button and open the new lakehouse.

![](./media/image8.png)

![](./media/image9.png)

4.  You will see a notification stating **Successfully created SQL
    endpoint**.

![](./media/image10.png)

### Task 3: Ingest sample data

1.  On the **IQ_Lakehouse** page, navigate to the **Get data in your
    lakehouse** section, and click on **Upload files**.

![](./media/image11.png)

2.  On the **Upload files** tab, click on the folder icon under
    **Files**.

![](./media/image12.png)

3.  Browse to **C:\LabFiles\Lab1** on your VM, select the
    **DimProducts.csv**, **DimStore.csv**, **FactSale.csv** and
    **Freezer.csv** files, and click on the **Open** button.

![](./media/image13.png)

4.  Click on the **Upload** button, and then close the **Upload files**
    dialog by selecting the **X** icon.

![](./media/image14.png)

![](./media/image15.png)

5.  Select **Files**. The files appear in the Files pane.

![](./media/image16.png)

6.  In the **Explorer** pane, select **Files**. Hover your mouse over
    the **DimProducts.csv** file, click on the horizontal ellipses
    **(…)** beside it, click on **Load Table**, and then select **New
    table**.

![](./media/image17.png)

![](./media/image18.png)

7.  In the **Load file to new table** dialog box, click on the **Load**
    button.

![](./media/image19.png)

8.  The **DimProducts** table is now successfully created.

![](./media/image20.png)

9.  Select the **DimProducts** table to preview the data.

**Note:** You may need to select the **Refresh** button more than once
to preview the data.

![](./media/image21.png)

10. Repeat steps 6 through 9 to load the remaining files
    (**DimStore.csv**, **FactSale.csv** and **Freezer.csv**) into
    tables.

![](./media/image22.png)

![](./media/image23.png)

![](./media/image24.png)

![](./media/image25.png)

![](./media/image26.png)

![](./media/image27.png)

![](./media/image28.png)

11. From the left navigation bar, select the **Fabric IQ
    Ontology@lab.LabInstance.Id** workspace.

![](./media/image29.png)

### Task 4: Prepare the eventhouse

Follow these steps to upload the device streaming data file to a KQL
database in Eventhouse.

1.  On the workspace page, select **+ New item** and select
    **Eventhouse**.

![](./media/image30.png)

2.  Name the eventhouse +++TelemetryDataEH+++ and click on the
    **Create** button.

![](./media/image31.png)

3.  The eventhouse opens when it's ready.

![](./media/image32.png)

4.  Open the KQL database by selecting its name.

![](./media/image33.png)

![](./media/image34.png)

5.  On the lower ribbon of your **KQL database**, click on **Get data**,
    and then select **Local file** to upload files from your local
    system into the database.

![](./media/image35.png)

6.  Select the option to ingest data into a new table, click on **+ New
    table**, and enter +++FreezerTelemetry+++ as the table name.

![](./media/image36.png)

![](./media/image37.png)

7.  Select the destination table, then drag and drop the file or click
    on **Browse for files** to upload the data.

![](./media/image38.png)

8.  Browse to **C:\LabFiles\Lab1** on your VM, select the
    **FreezerTelemetry.csv** file, and click on the **Open** button.

![](./media/image39.png)

9.  Click on the **Next** button.

![](./media/image40.png)

10. Click on the **Finish** button.

![](./media/image41.png)

11. Wait for the data ingestion to be completed, and then click on
    **Close**.

![](./media/image42.png)

12. The KQL database shows the **FreezerTelemetry** table when you're
    done.

![](./media/image43.png)

13. Select the **Fabric IQ Ontology@lab.LabInstance.Id** workspace in
    the left navigation pane.

![](./media/image44.png)

## Exercise 2: Build an ontology from OneLake

In this exercise, you create an ontology item, add the Store, Products
and SaleEvent entity types, bind them to lakehouse tables, and create
relationships between them.

### Task 1: Create an ontology (preview) item

1.  In your Fabric workspace, select **+ New item**. Search for and
    select the **Ontology (preview)** item.

![](./media/image45.png)

2.  Enter +++RetailSalesOntology+++ as the **Name** of your ontology and
    click on **Create**.

![](./media/image46.png)

**Tip:** Ontology names can include numbers, letters, and underscores.
Don't use spaces or dashes.

3.  The ontology opens when it's ready.

![](./media/image47.png)

Next, you create entity types, data bindings, and relationships based
on data from your lakehouse tables.

### Task 2: Create entity types and data bindings

First, create entity types. Entity types represent types of objects in a
business. This task has three entity types: *Store*, *Products*, and
*SaleEvent*. After you create the entity types, you create their
properties by binding source data columns in the **IQ_Lakehouse**
lakehouse tables.

**Add the first entity type (Store)**

1.  From the top ribbon or the center of the configuration canvas,
    select **Add entity type**.

![](./media/image48.png)

2.  Enter +++Store+++ as the name of your entity type and select **Add
    Entity Type**.

![](./media/image49.png)

3.  The *Store* entity type is added to the configuration canvas, and
    the **Entity type configuration** pane is visible.

![](./media/image50.png)

4.  On the configuration canvas, select **...** next to the entity name
    and select **Bind data**.

![](./media/image51.png)

5.  Select **Add data binding \> Lakehouse table**.

![](./media/image52.png)

6.  Choose your data source. Select the **IQ_Lakehouse** lakehouse and
    click on **Next**.

![](./media/image53.png)

7.  Select the **dimstore** table and click on **Select**.

![](./media/image54.png)

8.  Fields from the source table populate the data binding
    configuration. Observe the sections of the configuration page:

- **Entity type key**: Identifies the field (or fields) that can be
  used to uniquely identify each record of ingested data.

- **Binding selection**: Identifies the source table that holds the
  data for the binding.

- **Entity type key mapping**: Identifies the column(s) in the source
  data table that map to the entity type key property. You can select
  string and integer columns from your source data as the entity type
  key. Together, the columns you select uniquely identify a record.

- **Properties**: Lists the columns from the source data that will be
  represented as properties on the *Store* entity type. The **Source
  column** side populates automatically with the columns from the
  *dimstore* table, and the **Property name** side lists their
  corresponding property names on the *Store* entity type. For this
  lab, keep the default property names.

![](./media/image55.png)

9.  Select **Define entity type key** at the top of the configuration.

![](./media/image56.png)

10. Select **StoreId** from the property list and click on **Save**.

![](./media/image57.png)

11. **Save** the data binding.

![](./media/image58.png)

![](./media/image59.png)

12. Confirm that the entity type updated successfully, then select
    **Cancel** to close the configuration options.

![](./media/image60.png)

13. You see the **Configure** page of the entity type details. This page
    surfaces important information about the entity type, including its
    properties and data bindings. View your configured data bindings.

![](./media/image61.png)

14. Select **Home** to return to the configuration canvas and add new
    entity types.

![](./media/image62.png)

**Add the other entity types (Products, SaleEvent)**

15. Follow the same steps that you used for the **Store** entity type to
    create the entity types described in the following table. Each
    entity has a static data binding with the default columns from its
    source table. Start with **Products**.

| **Entity type name** | **Source table in IQ_Lakehouse** | **Entity type key** |
|----|----|----|
| +++Products+++ | **dimproducts** | **ProductId** |
| +++SaleEvent+++ | **factsales** | **SaleId** |

**Note:** Use the plural form **Products** to avoid a conflict with the
GQL reserved word **PRODUCT**.

![](./media/image63.png)

![](./media/image64.png)

![](./media/image65.png)

![](./media/image66.png)

![](./media/image67.png)

![](./media/image68.png)

![](./media/image69.png)

![](./media/image70.png)

![](./media/image71.png)

![](./media/image72.png)

![](./media/image73.png)

16. Select **Home** to return to the configuration canvas and add the
    **SaleEvent** entity type.

![](./media/image74.png)

![](./media/image75.png)

![](./media/image76.png)

![](./media/image77.png)

![](./media/image78.png)

![](./media/image79.png)

![](./media/image80.png)

![](./media/image81.png)

![](./media/image82.png)

![](./media/image83.png)

![](./media/image84.png)

![](./media/image85.png)

17. When you're done, you see these entity types listed in the **Entity
    Types** pane.

![](./media/image86.png)

### Task 3: Create relationship types

Next, create relationship types between the entity types to represent
contextual connections in your data.

**SaleEvent from Store**

1.  Select the **SaleEvent** entity type from the **Explorer**.

![](./media/image87.png)

2.  Select **Add relationship** from the menu ribbon.

![](./media/image88.png)

3.  Enter the following relationship type details and select **Add
    relationship type**.

| **Relationship type name** | +++from+++ |
|----|----|
| **Source entity type** | **SaleEvent** |
| **Target entity type** | **Store** |

![](./media/image89.png)

![](./media/image90.png)

4.  The relationship is added to the canvas. Select it to open the
    relationship details configuration. Observe the sections of the
    configuration page:

- **Origin entity type**: Lists details of the origin entity
  (**SaleEvent** in this case).

- **Relationship type**: Sets details of the relationship type.

- **Target entity type**: Lists details of the target entity
  (**Store** in this case).

![](./media/image91.png)

![](./media/image92.png)

5.  In the middle section, for **Mapping table**, select **Browse
    available sources** and select the **factsales** table. This table
    can link *Store* and *SaleEvent* entities together, because it
    contains identifying information for both entity types. Each row in
    this table references a store and a sale event by ID.

![](./media/image93.png)

6.  For **Matched SaleEvent: SaleId**, select **SaleId**. This setting
    specifies the column in the relationship source data table whose
    values match the key property defined on the *SaleEvent* entity. In
    this case, the relationship data source and the entity data source
    both use the *factsales* table, so you're selecting the same column
    (SaleId).

7.  For **Matched Store: StoreId**, select **StoreId**. This setting
    specifies the column in the relationship source data table
    (*factsales \> StoreId*) whose values match the key property defined
    on the *Store* entity (*dimstore \> StoreId*). In the lab data, the
    column name is the same (StoreId) in both tables.

![](./media/image94.png)

**Important:** Make sure to select the correct **Matched** columns that
match the entity type key properties.

8.  **Save** the relationship type. Confirm that the relationship type
    updated successfully, then select **Cancel** to close the
    configuration options.

![](./media/image95.png)

![](./media/image96.png)

![](./media/image97.png)

The first relationship is now created and bound to data in your source
table.

**SaleEvent sold Products**

9.  Select **Home** to return to the configuration canvas.

![](./media/image98.png)

10. Follow the same steps that you used for the first relationship type
    to create a second relationship from the **SaleEvent** entity type
    with the details in the following table.

| **Relationship type name** | **Origin entity type** | **Target entity type** | **Mapping table** | **Matched SaleEvent: SaleId** | **Matched Products: ProductId** |
|----|----|----|----|----|----|
| +++sold+++ | SaleEvent | Products | factsales | SaleId | ProductId |

![](./media/image99.png)

![](./media/image100.png)

![](./media/image101.png)

![](./media/image102.png)

![](./media/image103.png)

![](./media/image104.png)

![](./media/image105.png)

## Exercise 3: Enrich the ontology with additional data

In this exercise, you enrich your ontology by adding a new **Freezer**
entity type. This entity type adds more domain context and introduces
properties for time-series data, which reflects live operational
information. Finally, you create a new relationship type to represent
the connection between a store and its freezers.

**Note:** For both static and time-series data, you can create
properties without binding data and bind data later, or create
properties and bind data to them in a single step. This exercise
demonstrates both approaches.

### Task 1: Create the Freezer entity type and add properties

Follow these steps to create the *Freezer* entity type and add
properties to it. The properties aren't bound to data yet.

1.  Select **Add entity type** from the top ribbon. Enter +++Freezer+++
    as the name of your entity type and select **Add Entity Type**.

![](./media/image106.png)

![](./media/image107.png)

2.  With the **Freezer** entity type selected in the **Explorer**,
    select **View entity type details** from the top ribbon.

![](./media/image108.png)

3.  The **Configure** page of the entity type details opens. Expand
    **Manage property bindings** and select **Add properties**.

![](./media/image109.png)

4.  Add the following properties and click on **Save**.

| **Name** | **Property type** |
|----|----|
| +++FreezerId+++ | String |
| +++Model+++ | String |
| +++minSafeTempC+++ | Double |
| +++StoreId+++ | String |

![](./media/image110.png)

**Note:** Property names must be unique across all entity types.

![](./media/image111.png)

5.  The properties are added to the **Configure** page, unbound to any
    data source.

![](./media/image112.png)

### Task 2: Bind static data to properties

Next, bind static data to the properties you created on the *Freezer*
entity type.

1.  Expand **Manage property bindings** and select **Add binding and
    properties**.

![](./media/image113.png)

2.  Select **Add data binding \> Lakehouse table**.

![](./media/image114.png)

3.  Choose your data source. Select the **IQ_Lakehouse** lakehouse and
    click on **Next**, and then select the **freezer** table and click on
    **Select**.

![](./media/image115.png)

![](./media/image116.png)

4.  Fields from the source table populate the data binding
    configuration. As with the Store entity type, review the **Entity
    type key**, **Binding selection**, **Entity type key mapping** and
    **Properties** sections. The **Source column** side populates
    automatically with the columns from the **freezer** table, and the
    **Property name** side lists their corresponding property names on
    the **Freezer** entity type. For this lab, keep the default property
    names.

![](./media/image117.png)

5.  Select **Define entity type key** at the top of the configuration.
    Select **FreezerId** from the property list and click on **Save**.

![](./media/image118.png)

![](./media/image119.png)

6.  **Save** the data binding. Confirm that the entity type updated
    successfully, then select **Cancel** to close the configuration
    options.

![](./media/image120.png)

![](./media/image121.png)

### Task 3: Bind time-series data to additional properties

Next, add time-series data to the **Freezer** entity by creating new
properties and binding time-series data to them in a single data
binding operation.

1.  On the **Configure** page, expand **Manage property bindings** and
    select **Add binding and properties** again to reopen the binding
    configuration.

![](./media/image122.png)

2.  Under **Binding selection**, expand **Add data binding** and select
    **Eventhouse table or materialized view**.

![](./media/image123.png)

3.  Choose your data source. Select the **TelemetryDataEH** eventhouse
    and click on **Add**.

![](./media/image124.png)

4.  Select the **FreezerTelemetry** table and click on **Add**.

![](./media/image125.png)

5.  A **Timeseries data** section appears in the configuration. For
    **Timestamp column**, select **timestamp**.

![](./media/image126.png)

6.  Scroll down to the **Properties** section, where **StoreId** shows
    an error because it is already bound in the static data binding.
    Use the trash icon to delete the duplicated property.

![](./media/image127.png)

7.  **Save** the data binding. Confirm that the entity type updated
    successfully, then select **Cancel** to close the configuration
    options.

![](./media/image128.png)

![](./media/image129.png)

8.  Back on the **Configure** page for *Freezer*, notice that there are
    now more entity type properties, and the new ones are bound to the
    *FreezerTelemetry* data source.

![](./media/image130.png)

Now the *Freezer* entity has two data bindings: one with static data
from the *freezer* lakehouse table and one with streaming data from the
*FreezerTelemetry* eventhouse table.

### Task 4: Add the Store operates Freezer relationship type

Finally, create a new relationship type to represent the connection
between a store and its freezers.

1.  On the **Configure** page, expand **Manage relationships** and
    select **Add new relationship**.

![](./media/image131.png)

2.  Enter the following relationship type details and select **Add
    relationship type**.

| **Relationship type name** | +++operates+++ |
|----|----|
| **Source entity type** | **Store** |
| **Target entity type** | **Freezer** |

![](./media/image132.png)

3.  The relationship is added to the **Relationships** section. Select
    the **operates** relationship on the canvas to open the relationship
    details configuration. Observe the **Origin entity type** (*Store*),
    **Relationship type**, and **Target entity type** (*Freezer*)
    sections.

![](./media/image133.png)

![](./media/image134.png)

4.  In the middle section, enter the following details:

- **Mapping table**: Select the **freezer** table. This table can link
  **Store** and **Freezer** entities together, because each row
  references a store and a freezer by ID.

- **Matched Store: StoreId**: Select **StoreId**. This column
  (*freezer \> StoreId*) matches the key property defined on the
  *Store* entity (*dimstore \> StoreId*).

- **Matched Freezer: FreezerId**: Select **FreezerId**. The
  relationship data source and the entity data source both use the
  *freezer* table, so you're selecting the same column (FreezerId).

![](./media/image135.png)

**Important:** Make sure to select the correct source columns that
match the entity type key properties.

5.  **Save** the relationship type. Confirm that the relationship type
    updated successfully, then select **Cancel** to close the
    configuration options.

![](./media/image136.png)

![](./media/image137.png)

6.  On the **Configure** page for the entity, the new relationship is
    visible in the **Relationships** section.

![](./media/image138.png)

## Exercise 4: View the ontology

In this exercise, you explore your ontology by using the preview
experience. You inspect entity instances that instantiate your entity
types with data, and explore graph-shaped context across sales and
device streaming data.

### Task 1: View the instance list and static data

When you bound data to your entity types in the previous exercises, the
ontology automatically created instances of those entities that are tied
to the source data rows. In this task, you view those entity instances.

1.  Start in the **Home** configuration canvas of the ontology. Select
    the **SaleEvent** entity type, and then select **View entity type
    details** from the top ribbon.

![](./media/image139.png)

2.  Open the **Instances** tab. Verify that it shows six entity
    instances with data populated from the **factsales** lakehouse
    table, like revenue and unit counts.

![](./media/image140.png)

### Task 2: View time-series data

1.  In the top-left corner of the page, use the selector next to the
    entity type name to switch to the **Freezer** entity type.

![](./media/image141.png)

2.  Open the **Overview** tab. The tab loads with empty charts, because
    the default time range of **Last 30 days** doesn't include any data.

![](./media/image142.png)

3.  Update the time range from the default of **Last 30 days** to a
    custom date range that begins on **Fri Aug 01 2025 at 12:00 AM**,
    ends on **Mon Aug 04 2025 at 12:00 AM**, and has a **Time
    granularity** of **5 minutes**.

![](./media/image143.png)

4.  Observe the time-series data that's now visible from several
    **Freezer** entity instances in the time window you selected.

![](./media/image144.png)

### Task 3: View the ontology graph

The **Overview** tab also contains a **Relationship graph**, which you
use to visualize your ontology as a graph of nodes and edges.

1.  Use the entity type selector to switch to the **SaleEvent** entity
    type. In the **Relationship graph** tile, select **Expand**.

![](./media/image145.png)

2.  The expanded graph view opens. Observe the relationships from the
    **SaleEvent** entity type to **Products** and **Store**.

![](./media/image146.png)

3.  Use the entity type selector to switch to the **Store** entity type,
    and expand its **Relationship graph**.

![](./media/image147.png)

4.  In the graph, observe the relationships that **Store** has with
    **Freezer** and **SaleEvent**. Then, select **Run query** in the
    query builder ribbon. This runs the default query and shows a graph
    of entity instances alongside their connections.

![](./media/image148.png)

![](./media/image149.png)

![](./media/image150.png)

### Task 4: Query graph instances

In the relationship graph view, you can query your ontology for entity
instances that meet certain criteria. Use the **Query builder** filters
in the top ribbon to craft queries.

![](./media/image151.png)

First, craft this query: **Show all freezers that are operated in the
Paris store.**

1.  In the *Store* entity's relationship graph, select **Add filter \>
    Store \> StoreId** from the query builder ribbon. Set the filter to
    **StoreId = +++S-PAR-01+++**. This value is the store ID for the
    Paris store.

![](./media/image152.png)

![](./media/image153.png)

2.  In the **Components** section, uncheck **SaleEvent** so that the
    only checked fields are **Nodes \> Store**, **Nodes \> Freezer**, and
    **Edges \> operates**.

![](./media/image154.png)

3.  Select **Run query** and verify that the instance graph shows two
    freezers connected to the **Paris** store.

![](./media/image155.png)

![](./media/image156.png)

4.  Select **Clear query** to clear the query results.

![](./media/image157.png)

Next, craft this query: **Show all stores that have made a sale with a
revenue greater than 150.**

5.  Select **Add a node** and add a node for **SaleEvent**.

![](./media/image158.png)

6.  In the **Components** section, check the boxes next to **Nodes \>
    Store** and **Edges \> from** to add them to the graph.

![](./media/image159.png)

7.  From the query builder ribbon, select **Add filter \> SaleEvent \>
    RevenueUSD**. Set the filter to **RevenueUSD \> +++150+++**.

![](./media/image160.png)

![](./media/image161.png)

8.  Select **Run query** and verify that the instance graph shows two
    stores that meet the filter for their connected sale events. You can
    also select the nodes in the graph to get details of the specific
    sale events.

![](./media/image162.png)

![](./media/image163.png)

This process lets you inspect the paths that connect operational issues
(like rising freezer temperature at certain stores) to business outcomes
(sales).

## Exercise 5: Consume the ontology from agents

Ontology (preview) integrates with [Fabric data agent
(preview)](https://learn.microsoft.com/en-us/fabric/data-science/concept-data-agent)
to let you ask questions in natural language and get answers grounded in
the ontology's definitions and bindings.

### Task 1: Create a data agent with an ontology (preview) source

1.  Click on the **Fabric IQ Ontology@lab.LabInstance.Id** workspace in
    the left navigation pane.

![](./media/image164.png)

2.  On the workspace page, select **+ New item**. In the **Filter by
    item type** search box, enter +++data agent+++ and select **Data
    agent**.

![](./media/image165.png)

3.  Enter +++RetailOntologyAgent+++ as the data agent name and click on
    **Create**.

![](./media/image166.png)

4.  On the **RetailOntologyAgent** page, select **Add a data source**.

![](./media/image167.png)

5.  On the **OneLake catalog** tab, select the **RetailSalesOntology**
    ontology and click on **Add**.

![](./media/image168.png)

6.  When the agent is ready, it opens.

![](./media/image169.png)

### Task 2: Provide agent instructions

**Note:** This step is added in response to a known issue affecting
aggregation in queries.

1.  Select **Agent instructions** from the menu ribbon.

![](./media/image170.png)

2.  At the bottom of the input box, add +++Support group by in GQL+++.
    This instruction enables better aggregation across ontology data.

![](./media/image171.png)

3.  The instruction is applied automatically. Optionally, close the
    **Agent instructions** tab.

![](./media/image172.png)

### Task 3: Query the agent with natural language

Next, explore your ontology with natural language questions.

1.  Enter the following text and click on the **Submit** icon.

> +++For each store, show any freezers operated by that store that ever had a humidity lower than 46 percent.+++

![](./media/image173.png)

![](./media/image174.png)

2.  Enter the following text and click on the **Submit** icon.

> +++What is the top product by revenue across all stores?+++

![](./media/image175.png)

![](./media/image176.png)

3.  Notice that the responses reference entity types (**Store**,
    **Products**, **Freezer**) and their relationships, not just raw
    tables.

![](./media/image177.png)

**Tip:** If you see errors that say there's no data while running the
example queries, wait a few minutes to give the agent more time to
initialize. Then, run the queries again.

Continue exploring the data agent by trying out some prompts of your
own.

## Exercise 6: Build and test a companion app with Project Rayfin

In this exercise, you use Project Rayfin to scaffold a Todo app, run it
locally, and deploy it to your Fabric workspace.

### Task 1: Build and test the app locally

1.  Open **File Explorer**, go to the **C:\\** drive, click on **New** on
    the toolbar, select **Folder**, type +++Lab4+++ as the folder name,
    and press **Enter**.

![](./media/image178.png)

2.  In the Windows search box, type +++Visual Studio Code+++, and then
    click on **Visual Studio Code**.

![](./media/image179.png)

3.  In the Visual Studio Code dialog box, click on **Allow** to continue
    the Microsoft authentication process.

![](./media/image180.png)

4.  In the **Sign in** window, select **Work or school account**, and
    then click on **Continue**.

![](./media/image181.png)

5.  Sign in with the following credentials.

| **Username** | **+++@lab.CloudPortalCredential(User1).Username+++** |
|----|----|
| **Password** | **+++@lab.CloudPortalCredential(User1).Password+++** |

![](./media/image182.png)

![](./media/image183.png)

6.  In Visual Studio Code, click on the **More Actions (...)** menu,
    select **Terminal**, and then choose **New Terminal** to open a new
    integrated terminal window.

![](./media/image184.png)

7.  In the terminal, navigate to the **Lab4** directory.

> +++cd C:\Lab4+++

![](./media/image185.png)

8.  Run the following command to scaffold the **\[Experimental\] Todo
    app with full local dev** template.

```powershell
npm create @microsoft/rayfin@latest -- --template https://github.com/microsoft/awesome-rayfin --template-name "[Experimental] Todo app with full local dev"
```

![](./media/image186.png)

![](./media/image187.png)

9.  Enter +++Fabricapp+++ as the project name.

![](./media/image188.png)

![](./media/image189.png)

10. After the project is created successfully, navigate to the
    **Fabricapp** project directory and start the local development
    server.

> +++cd Fabricapp+++
>
> +++npm run dev+++

![](./media/image190.png)

11. When prompted, enter the Fabric workspace name
    +++Fabric IQ Ontology@lab.LabInstance.Id+++ and press **Enter** to
    continue deploying the application to the selected Fabric workspace.

![](./media/image191.png)

12. When the **Windows Security** dialog appears, click on **Allow** to
    permit **Node.js JavaScript Runtime** to communicate on public and
    private networks.

![](./media/image192.png)

13. Copy the local frontend URL shown in the terminal, which should be
    similar to +++http://localhost:5173+++, and open it in a new browser
    tab.

![](./media/image193.png)

14. Select the **Sign in with Microsoft** button. Since you already have
    an active SSO session from Exercise 1, you should be signed in
    automatically. Otherwise, sign in with the same Microsoft account
    you used for Fabric:

| **Email** | **+++@lab.CloudPortalCredential(User1).Username+++** |
|----|----|
| **TAP** | **+++@lab.CloudPortalCredential(User1).AccessToken+++** |

![](./media/image194.png)

15. In the **Todo App**, enter
    +++Review Lakeshore Retail ontology relationships+++ in the task
    field, and then click on **Add** to create the new to-do item.

![](./media/image195.png)

16. Enter +++Validate freezer telemetry ingestion+++ in the task field,
    and then click on **Add** to create another to-do item.

![](./media/image196.png)

![](./media/image197.png)

17. Select one of the tasks.

![](./media/image198.png)

![](./media/image199.png)

### Task 2: Deploy the app to Fabric

1.  Back in the Visual Studio Code terminal, stop the Vite dev server by
    pressing **Ctrl+C**.

![](./media/image200.png)

2.  Run the following command to deploy the app to your Fabric
    workspace.

> +++npm run up+++

![](./media/image201.png)

3.  When the deployment finishes, the CLI prints a **static hosting
    URL**, similar to **https://{random-prefix}.webapp.rayfin….com**.
    Click on the **App URL** to launch the application.

![](./media/image202.png)

4.  When the **Do you want Code to open the external website?** prompt
    appears, click on **Open** to launch the deployed application in
    your default web browser.

![](./media/image203.png)

5.  Select **Sign in with Microsoft**, just like you did in Task 1.

![](./media/image204.png)

![](./media/image205.png)

### Task 3: Inspect the deployment in Fabric

Let's take a look at the deployed app and database in the Microsoft
Fabric portal.

1.  Open the Microsoft Fabric portal at
    +++https://app.fabric.microsoft.com+++.

2.  Open the **Fabric IQ Ontology@lab.LabInstance.Id** workspace you
    created in Exercise 1.

![](./media/image206.png)

3.  Confirm that the workspace contains a **Fabric data app** item and a
    **SQL Database** item.

![](./media/image207.png)

![](./media/image208.png)

## Exercise 7: Clean up resources

1.  Select your workspace, **Fabric IQ Ontology@lab.LabInstance.Id**,
    from the left-hand navigation menu. It opens the workspace item
    view.

![](./media/image209.png)

2.  Select the **...** option under the workspace name and select
    **Workspace settings**.

![](./media/image210.png)

3.  Navigate to the bottom of the **General** tab and select **Remove
    this workspace**.

![](./media/image211.png)

4.  Click on **Delete** in the warning that pops up.

![](./media/image212.png)

**Summary**

In this lab, you used Microsoft Fabric IQ Ontology (preview) to create a
connected, semantic data model that represents real-world business
concepts and their relationships. By combining structured lakehouse data
with streaming telemetry data, the ontology provides a unified,
business-friendly view of enterprise data.

Through entity definitions, data bindings, and relationship modeling,
you analyzed how operational signals — such as freezer temperature or
humidity — relate to business outcomes like sales and revenue. You
explored the ontology with graph queries, connected it to a Fabric data
agent for natural language questions, and built and deployed a
companion app to your Fabric workspace with Project Rayfin. These
skills show how Fabric IQ Ontology helps bridge operational data and
analytics, supporting smarter decision-making across domains.

