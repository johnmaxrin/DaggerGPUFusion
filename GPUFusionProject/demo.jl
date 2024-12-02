using Dagger
using DaggerGPU
import DaggerGPU: Kernel
using KernelAbstractions
using CUDA

@kernel function kernel1(A)
    idx = @index(Global, Linear)
    A[idx] += 1
end
@kernel function kernel2(A, B)
    idx = @index(Global, Linear)
    B[idx] = A[idx]
end

@kernel function kernel3()
    p = 5+8
end

A = CUDA.rand(Float32, 10)
B = CUDA.rand(Float32, 10)
C = CUDA.rand(Float32, 10)

scope = Dagger.scope(cuda_gpu=1)

Dagger.gpufuse() do
    Dagger.spawn_sequential() do
        t1 = Dagger.@spawn scope=scope Kernel(kernel1)(A)
        t2 = Dagger.@spawn scope=scope Kernel(kernel2)(A,B)
        t3 = Dagger.@spawn scope=scope Kernel(kernel1)(C)
    end
end
