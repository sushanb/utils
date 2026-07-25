package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var (
	goGVR = schema.GroupVersionResource{
		Group:    "bigtable.sushanb.com",
		Version:  "v1",
		Resource: "bigtableworkloads",
	}
	workerImage = os.Getenv("WORKER_IMAGE") // Passed via Env Var

	//
	javaGVR = schema.GroupVersionResource{
		Group:    "java.sushanb.com", // Assumed new group
		Version:  "v1",
		Resource: "javaworkloads",
	}

	javaWorkerImage = os.Getenv("JAVA_WORKER_IMAGE") // New

	validationGVR = schema.GroupVersionResource{
		Group:    "bigtable.sushanb.com",
		Version:  "v1",
		Resource: "bigtablevalidationworkloads",
	}

	validationWorkerImage = os.Getenv("VALIDATION_WORKER_IMAGE")
)

func main() {
	if workerImage == "" {
		log.Fatal("WORKER_IMAGE env var is required")
	}

	// 1. Connect to Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fallback to local config for testing
		home := homedir.HomeDir()
		kubeconfig := filepath.Join(home, ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Error building kubeconfig: %v", err)
		}
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// Standard client for creating Deployments
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Start watching resources
	log.Println("Starting BigtableWorkload Controller...")
	log.Println("sushan")

	// Shared factory
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, time.Minute)

	// go workload watcher
	informer := factory.ForResource(goGVR).Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			reconcileGoWorkload(clientset, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			reconcileGoWorkload(clientset, new)
		},
		DeleteFunc: func(obj interface{}) {
			// In a real operator, you might clean up here.
			// Kubernetes OwnerReferences usually handle cleanup automatically.
			log.Println("Resource deleted")
		},
	})

	// --- Watcher 2: Java Workloads (NEW) ---
	javaInformer := factory.ForResource(javaGVR).Informer()
	javaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			reconcileJava(clientset, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			reconcileJava(clientset, new)
		},
		DeleteFunc: func(obj interface{}) {
			log.Println("JavaWorkload deleted")
		},
	})

	// --- Watcher 3: Bigtable Validation Workloads ---
	validationInformer := factory.ForResource(validationGVR).Informer()
	validationInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			reconcileValidation(clientset, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			reconcileValidation(clientset, new)
		},
		DeleteFunc: func(obj interface{}) {
			log.Println("BigtableValidationWorkload deleted")
		},
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	factory.Start(ctx.Done())
	<-ctx.Done()
}

func reconcileGoWorkload(clientset *kubernetes.Clientset, obj interface{}) {
	u := obj.(*unstructured.Unstructured)
	name := u.GetName()
	namespace := u.GetNamespace()

	// Parse fields
	spec := u.Object["spec"].(map[string]interface{})
	projectID, _ := spec["projectID"].(string)
	instanceID, _ := spec["instanceID"].(string)
	region, _ := spec["region"].(string)
	replicas := int32(spec["replicas"].(int64))

	serviceAccount, _ := spec["serviceAccountName"].(string)

	if serviceAccount == "" {
		serviceAccount = "bigtable-worker-sa"
	}

	parsedEnv := parseEnv(spec)

	deploymentName := fmt.Sprintf("%s-bt-worker", name)
	log.Printf("[Bigtable] Reconciling: %s | Instance: %s", name, instanceID)

	desiredContainer := corev1.Container{
		Name:            "worker",
		Image:           workerImage,
		ImagePullPolicy: corev1.PullAlways,
		Args: []string{
			"-project=" + projectID,
			"-instance=" + instanceID,
		},
		Env: parsedEnv,
	}

	// Owner Reference for Bigtable CR
	ownerRef := *metav1.NewControllerRef(u, schema.GroupVersionKind{
		Group: "bigtable.sushanb.com", Version: "v1", Kind: "BigtableWorkload",
	})

	ensureDeployment(clientset, namespace, deploymentName, replicas, region, desiredContainer, ownerRef, serviceAccount)
}

func reconcileValidation(clientset *kubernetes.Clientset, obj interface{}) {
	u := obj.(*unstructured.Unstructured)
	name := u.GetName()
	namespace := u.GetNamespace()

	spec := u.Object["spec"].(map[string]interface{})
	projectID, _ := spec["projectID"].(string)
	instanceID, _ := spec["instanceID"].(string)
	region, _ := spec["region"].(string)
	replicas := int32(spec["replicas"].(int64))

	serviceAccount, _ := spec["serviceAccountName"].(string)
	if serviceAccount == "" {
		serviceAccount = "bigtable-worker-sa"
	}

	parsedEnv := parseEnv(spec)

	deploymentName := fmt.Sprintf("%s-bt-validator", name)
	log.Printf("[Validation] Reconciling: %s | Instance: %s", name, instanceID)

	desiredContainer := corev1.Container{
		Name:            "validator",
		Image:           validationWorkerImage,
		ImagePullPolicy: corev1.PullAlways,
		Args: []string{
			"-project=" + projectID,
			"-instance=" + instanceID,
		},
		Env: parsedEnv,
	}

	ownerRef := *metav1.NewControllerRef(u, schema.GroupVersionKind{
		Group: "bigtable.sushanb.com", Version: "v1", Kind: "BigtableValidationWorkload",
	})

	ensureDeployment(clientset, namespace, deploymentName, replicas, region, desiredContainer, ownerRef, serviceAccount)
}

func reconcileJava(clientset *kubernetes.Clientset, obj interface{}) {
	u := obj.(*unstructured.Unstructured)
	name := u.GetName()
	namespace := u.GetNamespace()

	// Parse fields specific to JavaWorkload
	spec := u.Object["spec"].(map[string]interface{})

	projectID, _ := spec["projectID"].(string)
	instanceID, _ := spec["instanceID"].(string)
	tableID, _ := spec["tableID"].(string)

	region, _ := spec["region"].(string)

	// Example Java fields: maybe users specify JVM args or a jar path
	jvmArgs, _ := spec["jvmArgs"].(string)
	appArgs, _ := spec["appArgs"].(string)
	replicas := int32(spec["replicas"].(int64))

	serviceAccount, _ := spec["serviceAccountName"].(string)

	if serviceAccount == "" {
		serviceAccount = "bigtable-worker-sa"
	}

	parsedEnv := parseEnv(spec)

	deploymentName := fmt.Sprintf("%s-java-app", name)
	log.Printf("[Java] Reconciling: %s | JVM Args: %s", name, jvmArgs)

	// Construct command for Java
	containerArgs := []string{"-jar", "/app/app.jar"}
	if jvmArgs != "" {
		containerArgs = append([]string{jvmArgs}, containerArgs...)
	}
	// Add project/instance as flags to the Java app
	containerArgs = append(containerArgs,
		fmt.Sprintf("-project=%s", projectID),
		fmt.Sprintf("-instance=%s", instanceID),
		fmt.Sprintf("-table=%s", tableID),
	)

	if appArgs != "" {
		containerArgs = append(containerArgs, appArgs)
	}

	desiredContainer := corev1.Container{
		Name:            "java-worker",
		Image:           javaWorkerImage, // Different image
		ImagePullPolicy: corev1.PullAlways,
		Args:            containerArgs,
		Env:             parsedEnv,
	}

	// Owner Reference for Java CR
	ownerRef := *metav1.NewControllerRef(u, schema.GroupVersionKind{
		Group: "java.sushanb.com", Version: "v1", Kind: "JavaWorkload",
	})

	// Re-use the deployment logic, passing nil for region if not needed
	ensureDeployment(clientset, namespace, deploymentName, replicas, region, desiredContainer, ownerRef, serviceAccount)
}

// parseEnv extracts environment variables from spec (Common logic)
func parseEnv(spec map[string]interface{}) []corev1.EnvVar {
	var parsedEnv []corev1.EnvVar
	if envRaw, found := spec["env"]; found {
		if envList, ok := envRaw.([]interface{}); ok {
			for _, item := range envList {
				if itemMap, ok := item.(map[string]interface{}); ok {
					eName, _ := itemMap["name"].(string)
					eValue, _ := itemMap["value"].(string)
					if eName != "" {
						parsedEnv = append(parsedEnv, corev1.EnvVar{
							Name:  eName,
							Value: eValue,
						})
					}
				}
			}
		}
	}
	return parsedEnv
}

// ensureDeployment handles the Create/Update logic for standard Deployments
func ensureDeployment(clientset *kubernetes.Clientset, ns, name string, replicas int32, region string, container corev1.Container, owner metav1.OwnerReference, serviceAccount string) {
	existing, err := clientset.AppsV1().Deployments(ns).Get(context.TODO(), name, metav1.GetOptions{})

	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Printf("Creating deployment %s...", name)
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:            name,
					Namespace:       ns,
					OwnerReferences: []metav1.OwnerReference{owner},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
						Spec: corev1.PodSpec{
							ServiceAccountName: serviceAccount,
							Containers:         []corev1.Container{container},
						},
					},
				},
			}
			if region != "" {
				deployment.Spec.Template.Spec.NodeSelector = map[string]string{"topology.kubernetes.io/region": region}
			}

			_, err = clientset.AppsV1().Deployments(ns).Create(context.TODO(), deployment, metav1.CreateOptions{})
			if err != nil {
				log.Printf("Error creating deployment: %v", err)
			}
			return
		}
		log.Printf("Error getting deployment: %v", err)
		return
	}

	// --- UPDATE LOGIC ---
	needsUpdate := false

	// Check Image
	if existing.Spec.Template.Spec.Containers[0].Image != container.Image {
		existing.Spec.Template.Spec.Containers[0].Image = container.Image
		needsUpdate = true
	}

	// Check Replicas
	if *existing.Spec.Replicas != replicas {
		existing.Spec.Replicas = &replicas
		needsUpdate = true
	}

	// Check Env
	currentEnv := existing.Spec.Template.Spec.Containers[0].Env
	if !reflect.DeepEqual(currentEnv, container.Env) {
		existing.Spec.Template.Spec.Containers[0].Env = container.Env
		needsUpdate = true
	}

	if needsUpdate {
		_, err = clientset.AppsV1().Deployments(ns).Update(context.TODO(), existing, metav1.UpdateOptions{})
		if err != nil {
			log.Printf("Failed to update deployment: %v", err)
		} else {
			log.Printf("Updated deployment %s successfully", name)
		}
	}
}
