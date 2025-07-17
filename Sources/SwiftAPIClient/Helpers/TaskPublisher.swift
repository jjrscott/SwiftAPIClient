//
//  TaskPublisher.swift
//
//  Created by John Scott on 17/07/2025.
//

import Combine

class TaskPublisher<Output, Failure>: Publisher where Failure: Error {
    private let resultPublisher: PassthroughSubject<Output, Failure>
    private var task: Task<Void, Never>

    public init(operation: @escaping () async throws(Failure) -> Output) {
        self.resultPublisher = .init()
        task = Task { [resultPublisher] in
            do throws(Failure) {
                let result = try await operation()
                resultPublisher.send(result)
            } catch {
                resultPublisher.send(completion: .failure(error))
            }
        }
    }
    
    public func receive<S: Subscriber>(subscriber: S) where S.Input == Output, S.Failure == Failure {
        resultPublisher.receive(subscriber: subscriber)
    }

    deinit {
        task.cancel()
    }
}
