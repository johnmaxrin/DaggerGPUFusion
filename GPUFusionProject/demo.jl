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

A = CUDA.rand(Float32, 1000)
B = CUDA.rand(Float32, 1000)

scope = Dagger.scope(cuda_gpu=1)

Dagger.gpufuse() do
    t1 = Dagger.@spawn scope=scope Kernel(kernel1)(A)
    t2 = Dagger.@spawn scope=scope Kernel(kernel2)(A, B)
end
