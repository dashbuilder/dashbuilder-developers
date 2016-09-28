# Dashbuilder for developers

This repository provides several reference implementations and usage examples around the Dashbuilder APIs.

The modules available are:

* **dataset-api-examples**: Some test clases ilustrating for instance, how to register data set definitions (both bean generated and CSV based), how to execute data lookup calls over a data set, or how to activate the DatasetDeployer component to autoregister datasets from a specific directory.

* **dataprovider-template**: For those developers interested in implementing a custom data provider. It is a maven template project containing the required classes of a typical data provider implementation. Developers can copy this template and modify the provided classes with their own implementation logic. Once it's done, it is only a matter of building the maven module and add the generated .jar archive to the dashbuilder webapp. 

