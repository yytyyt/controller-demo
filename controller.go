package main

import (
	"fmt"
	crd1beta1 "github.com/yyt/controller-demo/pkg/apis/stable/v1beta1"
	informer "github.com/yyt/controller-demo/pkg/client/informers/externalversions/stable/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"time"
)

type Controller struct {
	workqueue workqueue.RateLimitingInterface
	informer  informer.CronTabInformer
}

func NewController(informer informer.CronTabInformer) *Controller {
	controller := &Controller{
		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "crontab-controller"),
		informer:  informer,
	}
	klog.Info("Setting up crontab controller")
	// 注册事件监听函数
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	return controller
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	// 停止控制器后需要关掉队列
	defer c.workqueue.ShutDown()

	// 启动控制器
	klog.Info("starting crontab controller")

	// 等待所有相关得缓存同步完成， 然后再开始处理workqueue中得数据
	if !cache.WaitForCacheSync(stopCh, c.informer.Informer().HasSynced) {
		return fmt.Errorf("time out waiting for cahchs to sync")
	}

	klog.Info("Informer caches to sync completed")

	// 启动worker处理元素
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	<-stopCh
	klog.Info("stopping crontab controller")
	return nil
}

// 处理元素
func (c *Controller) runWorker() {
	for c.processNextItem() {

	}

}

// 实现业务逻辑
func (c *Controller) processNextItem() bool {
	// 从 workqueue里面取出一个元素
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	// 根据key 去处理我们得业务逻辑
	err := func(obj interface{}) error {
		// 告诉队列我们已经处理了该key
		defer c.workqueue.Done(obj)

		var ok bool
		var key string
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			return fmt.Errorf("expected string in workqueue but get %#v ", obj)
		}
		// 业务逻辑
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("sync err %v ", err)
		}
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced %s", key)
		return nil
	}(obj)

	// 错误处理
	if err != nil {
		runtime.HandleError(err)
	}

	return true
}

// key --> crontab -->indexer
func (c *Controller) syncHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	// 错误处理
	if err != nil {
		return err
	}
	// 获取crontab 实际从Indexer中获取
	cronTab, err := c.informer.Lister().CronTabs(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			// 对应得crontab 对象已经被删除
			klog.Warningf("CronTab deleting:%s/%s", namespace, name)
			return nil
		}
		return err
	}
	// 以下具体业务处理
	klog.Infof("CronTab try to process:%#v", cronTab)
	return nil
}

func (c *Controller) onAdd(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
	}
	c.workqueue.AddRateLimited(key)
}

func (c *Controller) onUpdate(old, new interface{}) {
	oldObj := old.(*crd1beta1.CronTab)
	newObj := new.(*crd1beta1.CronTab)
	// 比较两个对象得资源版本是否一致
	if oldObj.ResourceVersion == newObj.ResourceVersion {
		return
	}
	c.onAdd(new)
}

func (c *Controller) onDelete(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
	}
	c.workqueue.AddRateLimited(key)
}
