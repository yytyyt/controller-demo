package main

import (
	"flag"
	"github.com/yyt/controller-demo/pkg/client/clientset/versioned"
	"github.com/yyt/controller-demo/pkg/client/informers/externalversions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func initClientSet() (*kubernetes.Clientset, *rest.Config, error) {
	var err error
	var config *rest.Config
	var kubeconfig *string
	if home := homeDire(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(可选)输入kubeconfig文件得绝对路径")

	} else {
		kubeconfig = flag.String("kubeconfig", "", "输入kubeconfig文件得绝对路径")
	}
	flag.Parse()

	// 首先使用inCluster模式 Pod 中
	config, err = rest.InClusterConfig()
	if err != nil {
		// 使用Kubeconfig创建集群配置
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			panic(err.Error())
		}
	}
	// 创建ClientSet对象
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	return clientset, config, nil
}

func homeDire() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE")
}

func setupSignalHandler() (stopCh <-chan struct{}) {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, []os.Signal{os.Interrupt, syscall.SIGALRM}...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1)
	}()
	return stop
}

func main() {
	_, config, err := initClientSet()
	if err != nil {
		klog.Fatalf("Error init kubernetes client:%s", err.Error())
	}

	// 实例化一个 CronTab得clientset
	crontabClientSet, err := versioned.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error init kubernetes crontabClient:%s", err.Error())
	}

	stopCh := setupSignalHandler()

	// 实例化CronTab 得 informerFactory 工厂类
	sharedInformerFactory := externalversions.NewSharedInformerFactory(crontabClientSet, time.Second)

	// 实例化 CronTab控制器
	controller := NewController(sharedInformerFactory.Stable().V1beta1().CronTabs())

	// 启动Informer 执行ListAndWatch操作
	go sharedInformerFactory.Start(stopCh)

	// 启动控制器得控制循环
	err = controller.Run(1, stopCh)
	if err != nil {
		klog.Fatalf("Error running crontab controller:%s", err.Error())
	}

}
