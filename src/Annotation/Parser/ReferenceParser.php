<?php declare(strict_types=1);


namespace Six\Rpc\Client\Annotation\Parser;

use PhpDocReader\AnnotationException;
use PhpDocReader\PhpDocReader;
use ReflectionException;
use ReflectionProperty;
use Six\Rpc\Client\Proxy;
use Six\Rpc\Client\ReferenceRegister;
use Swoft\Annotation\Annotation\Mapping\AnnotationParser;
use Swoft\Annotation\Annotation\Parser\Parser;
use Swoft\Proxy\Exception\ProxyException;
use Six\Rpc\Client\Annotation\Mapping\Reference;
use Swoft\Rpc\Client\Exception\RpcClientException;
/**
 * Class ReferenceParser
 *
 * @since 2.0
 *
 * @AnnotationParser(Reference::class)
 */
class ReferenceParser extends Parser
{
    /**
     * @param int       $type
     * @param Reference $annotationObject
     *
     * @return array
     * @throws RpcClientException
     * @throws AnnotationException
     * @throws ReflectionException
     * @throws ProxyException
     */
    public function parse(int $type, $annotationObject): array
    {
        // Parse php document
        $phpReader       = new PhpDocReader();
        $reflectProperty = new ReflectionProperty($this->className, $this->propertyName);
        $propClassType   = $phpReader->getPropertyClass($reflectProperty);
        if (empty($propClassType)) {
            throw new RpcClientException(
                sprintf('`@Reference`(%s->%s) must to define `@var xxx`', $this->className, $this->propertyName)
            );
        }
        $className = Proxy::newClassName($propClassType);
        $this->definitions[$className] = [
            'class' => $className,
        ];
        ReferenceRegister::register($className, $annotationObject->getPool(), $annotationObject->getVersion());
        return [$className, true];
    }
}