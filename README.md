# aind-smartspim-pipeline-dispatcher

Repository that hosts the code, environment and metadata of the Code Ocean capsule used
to dispatch parallel image processing steps in the SmartSPIM pipeline using the Code Ocean pipeline feature.

This is necessary since we need to make sure we process SmartSPIM channels in parallel. In order to do this, we rely on the flatten connection between the results folder generated in this capsule and other capsules such as [aind-smartspim-segmentation](https://github.com/AllenNeuralDynamics/aind-SmartSPIM-segmentation), [aind-ccf-registration](https://github.com/AllenNeuralDynamics/aind-ccf-registration) and [aind-smartspim-quantification](https://github.com/AllenNeuralDynamics/aind-smartspim-quantification).